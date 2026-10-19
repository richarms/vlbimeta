"""Diagnostic, byte-preserving scan export for time/thread-ordered raw VDIF.

This initial exporter is for measuring the existing index-plus-export approach.
It does not replace controller publication, reconstruct missing packets, infer
execution, decode/requantise samples, or produce ANTAB/UVFLG. Out-of-order raw
frames fail explicitly: a general bounded-memory merge is separate work.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import sqlite3
import time
from typing import Any, Sequence

import numpy as np

from .runtime import product_paths, read_capture_manifest, write_metadata_json
from .vdif_index import CHUNK_BYTES, _fingerprint, plan_export


def _identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9]+", value):
        raise ValueError("Export experiment, station and scan IDs must be alphanumeric")
    return value


def export_scans(data_dir: Path, cbid: str, stream: str, index_dir: Path,
                 manifest_path: Path, output_dir: Path, *, experiment: str, station: str,
                 allow_partial: bool = False) -> dict[str, Any]:
    """Read/hash all raw bytes and copy whole frames to ordered per-scan files.

    Preserve every selected frame bit-for-bit, including its header/invalid flag.
    Require complete, valid nominal coverage unless partial export is explicitly
    enabled. Input must be strictly ordered by (frame time, thread ID) across
    shards in inventory order. Chunk memory is bounded independently of capture
    size. All output fsyncs and the final directory rename are timed.
    """
    start_wall, start_cpu = time.perf_counter(), time.process_time()
    experiment, station = _identifier(experiment), _identifier(station)
    paths = product_paths(data_dir, cbid, stream)
    capture = read_capture_manifest(paths)
    plan = plan_export(index_dir, manifest_path)
    manifest = json.loads(manifest_path.read_bytes())
    if manifest.get("experiment") not in (None, experiment):
        raise ValueError("Export experiment differs from manifest")
    if plan["capture_block_id"] != cbid or plan["stream_name"] != stream:
        raise ValueError("Index capture identity does not match raw capture")
    with sqlite3.connect((index_dir / "index.sqlite").resolve().as_uri() + "?mode=ro", uri=True) as db:
        index = json.loads(db.execute("SELECT value FROM metadata WHERE key='report'").fetchone()[0])
    if Path(index["raw_directory"]).resolve() != paths.vdif_dir.resolve():
        raise ValueError("Index points to a different raw directory")
    if [(s["name"], s["size_bytes"]) for s in index["shards"]] != [
            (s["name"], s["size_bytes"]) for s in capture["shards"]]:
        raise ValueError("Index inventory differs from raw capture")
    if not plan["scans"]:
        raise ValueError("No scheduled scans selected")
    for scan in plan["scans"]:
        _identifier(scan["schedule_scan_id"])
        if not allow_partial and not scan["complete_valid_frames"]:
            raise ValueError("Selected scan has missing/invalid frames; partial export requires opt-in")
    if output_dir.resolve() == paths.capture_root.resolve() or paths.capture_root.resolve() in output_dir.resolve().parents:
        raise ValueError("Export output must be outside the raw capture tree")
    staging = output_dir.with_name(output_dir.name + ".writing")
    if output_dir.exists() or output_dir.is_symlink() or staging.exists() or staging.is_symlink():
        raise FileExistsError("Export output/staging already exists; choose a fresh directory")
    fingerprints = {s["name"]: _fingerprint(paths.vdif_dir / s["name"]) for s in capture["shards"]}
    staging.mkdir(parents=True, exist_ok=False)
    geometry, fps = index["geometry"], index["frames_per_second"]
    frame_bytes = geometry["frame_bytes"]
    chunk_bytes = max(frame_bytes, CHUNK_BYTES // frame_bytes * frame_bytes)
    epochs = np.full(64, -1, dtype=np.int64)
    for epoch, tick in index["epoch_origin_ticks"].items():
        epochs[int(epoch)] = tick
    reports = []
    io_wall = hashing_cpu = selection_cpu = writing_wall = fsync_wall = 0.0
    bytes_read = bytes_written = 0
    last_key = None
    # The normal export has a small number of scan windows. Open only the active
    # output: a many-hour schedule does not retain one descriptor per scan.
    scan_index = 0
    output = None
    output_digest = None
    output_bytes = 0

    def close_output():
        nonlocal output, fsync_wall, output_bytes
        if output is None:
            return
        stamp = time.perf_counter()
        output.flush()
        os.fsync(output.fileno())
        fsync_wall += time.perf_counter() - stamp
        output.close()
        scan = plan["scans"][scan_index]
        if output_bytes != scan["selected_bytes"]:
            raise ValueError("Exported bytes differ from index selection")
        reports.append(dict(scan, relative_path=str(Path(output.name).relative_to(staging)),
                            size_bytes=output_bytes, sha256=output_digest.hexdigest()))
        output = None
        output_bytes = 0

    try:
        for shard in index["shards"]:
            digest = hashlib.sha256()
            path = paths.vdif_dir / shard["name"]
            with path.open("rb") as source:
                while True:
                    stamp = time.perf_counter()
                    chunk = source.read(chunk_bytes)
                    io_wall += time.perf_counter() - stamp
                    if not chunk:
                        break
                    bytes_read += len(chunk)
                    if len(chunk) % frame_bytes:
                        raise ValueError("Truncated raw VDIF frame")
                    cpu_stamp = time.process_time()
                    digest.update(chunk)
                    hashing_cpu += time.process_time() - cpu_stamp
                    cpu_stamp = time.process_time()
                    words = np.ndarray((len(chunk) // frame_bytes, 8), dtype="<u4",
                                       buffer=chunk, strides=(frame_bytes, 4))
                    epoch = (words[:, 1] >> 24) & 63
                    ticks = epochs[epoch] + (words[:, 0] & 0x3fffffff).astype(np.int64) * fps + (words[:, 1] & 0xffffff)
                    threads = (words[:, 3] >> 16) & 1023
                    keys = ticks * 1024 + threads
                    if (np.any(epochs[epoch] < 0) or np.any(keys[1:] <= keys[:-1])
                            or (last_key is not None and int(keys[0]) <= last_key)):
                        raise ValueError("Export requires strictly time/thread-ordered raw frames")
                    last_key = int(keys[-1])
                    selection_cpu += time.process_time() - cpu_stamp
                    view = memoryview(chunk)
                    while scan_index < len(plan["scans"]):
                        scan = plan["scans"][scan_index]
                        cpu_stamp = time.process_time()
                        first = int(np.searchsorted(ticks, scan["first_tick"], side="left"))
                        stop = int(np.searchsorted(ticks, scan["stop_tick"], side="left"))
                        selection_cpu += time.process_time() - cpu_stamp
                        if stop > first:
                            if output is None:
                                base = f"{experiment}_{station}_{scan['schedule_scan_id']}"
                                folder = staging / base
                                folder.mkdir()
                                output = (folder / (base + ".vdif")).open("xb")
                                output_digest = hashlib.sha256()
                            selected = view[first * frame_bytes:stop * frame_bytes]
                            stamp = time.perf_counter()
                            output.write(selected)
                            writing_wall += time.perf_counter() - stamp
                            cpu_stamp = time.process_time()
                            output_digest.update(selected)
                            hashing_cpu += time.process_time() - cpu_stamp
                            bytes_written += len(selected)
                            output_bytes += len(selected)
                        if int(ticks[-1]) >= scan["stop_tick"]:
                            close_output()
                            scan_index += 1
                        else:
                            break
            if digest.hexdigest() != shard["sha256"]:
                raise ValueError("Raw content differs from indexed SHA256")
        close_output()
        # Explicit empty results remain visible in the report when partial
        # export is requested. No invented replacement frames/files are made.
        existing = {scan["schedule_scan_id"] for scan in reports}
        for scan in plan["scans"]:
            if scan["schedule_scan_id"] not in existing:
                if scan["selected_bytes"]:
                    raise ValueError("Selected indexed frames were not exported")
                reports.append(dict(scan, relative_path=None, size_bytes=0, sha256=None))
        if read_capture_manifest(paths) != capture or any(
                _fingerprint(paths.vdif_dir / name) != fingerprint for name, fingerprint in fingerprints.items()):
            raise ValueError("Raw capture changed during export")
        report = dict(version=1, kind="diagnostic_scan_export", status="complete",
            capture_block_id=cbid, stream_name=stream, experiment=experiment, station=station,
            station_label_source="explicit argument, not inferred from VDIF station ID",
            science_validated=False, execution_status="unknown",
            selection_policy=plan["selection_policy"], scan_manifest_sha256=plan["scan_manifest_sha256"],
            raw_revalidated=True, selection_applied_to_vdif=True,
            raw_bytes=bytes_read, exported_bytes=bytes_written,
            frames_per_second=fps, geometry=geometry, scans=reports,
            ordering="VDIF frame time, then numeric thread ID; input order preserved",
            partial_export_allowed=allow_partial, invalid_frames_preserved=True,
            layout="<experiment>_<station>_<scan>/<experiment>_<station>_<scan>.vdif",
            shards=index["shards"])
        write_metadata_json(staging / "export.json", report)
        for path in [staging / "export.json"]:
            with path.open("rb") as source:
                os.fsync(source.fileno())
        # Sync newly created directory entries as well as file contents.
        stamp = time.perf_counter()
        for folder in [p for p in staging.iterdir() if p.is_dir()] + [staging]:
            fd = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        staging.rename(output_dir)
        fd = os.open(output_dir.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        fsync_wall += time.perf_counter() - stamp
        wall = time.perf_counter() - start_wall
        report["benchmark"] = dict(wall_seconds=wall, cpu_seconds=time.process_time() - start_cpu,
            raw_read_mib_per_second=bytes_read / 1024**2 / wall,
            selected_write_mib_per_second=bytes_written / 1024**2 / wall,
            combined_logical_io_mib_per_second=(bytes_read + bytes_written) / 1024**2 / wall,
            source_read_call_wall_seconds=io_wall, output_write_call_wall_seconds=writing_wall,
            fsync_wall_seconds=fsync_wall, hashing_cpu_seconds=hashing_cpu,
            header_selection_cpu_seconds=selection_cpu,
            process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        # Timing is diagnostic, outside the durable manifest/publication interval.
        write_metadata_json(output_dir / "benchmark.json", report["benchmark"])
        return report
    finally:
        if output is not None:
            output.close()


def main(argv: Sequence[str] | None = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--capture-block-id", required=True)
    parser.add_argument("--stream-name", required=True)
    parser.add_argument("--index-dir", type=Path, required=True)
    parser.add_argument("--scan-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--station", required=True)
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args(argv)
    result = export_scans(args.data_dir, args.capture_block_id, args.stream_name, args.index_dir,
        args.scan_manifest, args.output_dir, experiment=args.experiment, station=args.station,
        allow_partial=args.allow_partial)
    print(json.dumps(result["benchmark"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
