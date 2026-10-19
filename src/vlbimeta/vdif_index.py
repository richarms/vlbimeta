"""Index closed VDIF recordings and budget CSV scan exports without copying data.

This diagnostic is deliberately separate from product publication. Runs encode
consecutive frame times and a constant byte stride for one thread in one shard.
Memory use does not grow with frame count, even for disordered recordings.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from fractions import Fraction
import hashlib
import json
import math
from pathlib import Path
import re
import resource
import sqlite3
import struct
import time
from typing import Any, Sequence

from astropy.time import Time
from astropy.utils import iers

from .runtime import product_paths, read_capture_manifest, write_metadata_json

INDEX_VERSION = 1
HEADER = struct.Struct("<8I")
CHUNK_BYTES = 4 * 1024 * 1024


def utc_ticks(value: str, fps: int) -> Fraction:
    """Convert an aware ISO time to exact frame ticks on the continuous TAI axis.

    Datetime inputs have microsecond precision. Astropy is used only for integral
    seconds and leap offsets, never for per-frame floating-point arithmetic.
    Use the installed leap table without network access or global config changes.
    """
    if not isinstance(value, str):
        raise ValueError("Scan times must be ISO strings with a UTC offset")
    # Python 3.8-3.10 only accept 3/6 fractional digits; also reject precision
    # we cannot preserve instead of silently truncating it on newer Python.
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,6}))?(Z|[+-]\d\d:\d\d)", value)
    if match is None:
        raise ValueError("Scan times require a UTC offset and at most microsecond precision")
    fraction = "." + match[2].ljust(6, "0") if match[2] else ""
    stamp = datetime.fromisoformat(match[1] + fraction + match[3].replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("Scan times must have a UTC offset")
    stamp = stamp.astimezone(timezone.utc)
    with iers.conf.set_temp("auto_download", False):
        seconds = round(float(Time(stamp.replace(microsecond=0)).unix_tai))
    return (seconds + Fraction(stamp.microsecond, 1000000)) * fps


@dataclass
class Run:
    shard: str
    thread: int
    start_tick: int
    first_offset: int
    invalid: int
    count: int = 1
    stride: int = 0

    def append(self, tick: int, offset: int, invalid: int) -> bool:
        stride = offset - (self.first_offset + (self.count - 1) * self.stride)
        if (tick != self.start_tick + self.count or invalid != self.invalid
                or (self.count > 1 and stride != self.stride)):
            return False
        self.stride = stride
        self.count += 1
        return True

    def save(self, db: sqlite3.Connection) -> None:
        db.execute("INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?)", (
            self.shard, self.thread, self.start_tick, self.start_tick + self.count,
            self.first_offset, self.stride, self.invalid,
        ))


def _fingerprint(path: Path) -> tuple[int, ...]:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _geometry(words: tuple[int, ...]) -> dict[str, int]:
    w0, w1, w2, w3, w4, w5, w6, w7 = words
    length = (w2 & 0xFFFFFF) * 8
    # Initial profile: standard headers, EDV 0, fixed geometry. Other profiles
    # need explicit adapters/tests; guessing their time extensions is unsafe.
    if (w0 & (1 << 30) or w1 >> 30 or w2 >> 29 not in (0, 1)
            or any((w4, w5, w6, w7)) or not 40 <= length <= CHUNK_BYTES):
        raise ValueError("Unsupported VDIF header (standard EDV 0 profile required)")
    return {
        "frame_bytes": length, "header_bytes": 32, "version": w2 >> 29,
        "station_id": w3 & 0xFFFF, "bits_per_sample": ((w3 >> 26) & 31) + 1,
        "channels": 1 << ((w2 >> 24) & 31), "complex": w3 >> 31,
    }


def build_index(data_dir: Path, cbid: str, stream: str, output_dir: Path,
                fps: int, threads: Sequence[int]) -> dict[str, Any]:
    """Hash and index one closed capture in a single sequential read pass.

    Rate and expected threads must come from the instrument contract. EDV 0
    headers cannot establish them. A failed/interrupted output is never reused.
    """
    if type(fps) is not int or not 1 <= fps <= 1 << 24:
        raise ValueError("frames_per_second must be an integer in [1, 2**24]")
    if (not threads or len(set(threads)) != len(threads)
            or any(type(t) is not int or not 0 <= t <= 1023 for t in threads)):
        raise ValueError("Expected threads must be unique integers in [0, 1023]")
    expected = set(threads)
    paths = product_paths(data_dir, cbid, stream)
    capture = read_capture_manifest(paths)
    if paths.capture_root.resolve() in output_dir.resolve().parents:
        raise ValueError("Index output must be outside the raw capture tree")
    fingerprints = {s["name"]: _fingerprint(paths.vdif_dir / s["name"])
                    for s in capture["shards"]}
    output_dir.mkdir(parents=True, exist_ok=False)
    wall_start, cpu_start = time.perf_counter(), time.process_time()
    db = sqlite3.connect(str(output_dir / "index.sqlite"))
    try:
        db.executescript("""
            PRAGMA cache_size = -8192;
            PRAGMA temp_store = FILE;
            CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO metadata VALUES ('status', 'building');
            CREATE TABLE runs (shard TEXT NOT NULL, thread INTEGER NOT NULL,
                start_tick INTEGER NOT NULL, stop_tick INTEGER NOT NULL,
                first_offset INTEGER NOT NULL, stride INTEGER NOT NULL,
                invalid INTEGER NOT NULL);
        """)
        db.commit()
        geometry = None
        epoch_ticks: dict[int, int] = {}
        shards = []
        frame_count = 0
        for shard in capture["shards"]:
            path = paths.vdif_dir / shard["name"]
            active: dict[int, Run] = {}
            digest = hashlib.sha256()
            with path.open("rb") as source:
                # Peek at the first header; the main loop still reads/hashes all bytes.
                header = source.read(HEADER.size)
                if len(header) != HEADER.size:
                    raise ValueError(f"Truncated VDIF header: {path}")
                first_geometry = _geometry(HEADER.unpack(header))
                if geometry is None:
                    geometry = first_geometry
                frame_bytes = geometry["frame_bytes"]
                if first_geometry != geometry or shard["size_bytes"] % frame_bytes:
                    raise ValueError(f"Changed geometry or truncated VDIF shard: {path}")
                source.seek(0)
                chunk_bytes = (CHUNK_BYTES // frame_bytes) * frame_bytes
                offset = 0
                while True:
                    chunk = source.read(chunk_bytes)
                    if not chunk:
                        break
                    digest.update(chunk)
                    if len(chunk) % frame_bytes:
                        raise ValueError(f"Truncated VDIF frame: {path}")
                    for local_offset in range(0, len(chunk), frame_bytes):
                        words = HEADER.unpack_from(chunk, local_offset)
                        if _geometry(words) != geometry:
                            raise ValueError(f"VDIF geometry changed: {path}:{offset + local_offset}")
                        w0, w1, _, w3 = words[:4]
                        thread = (w3 >> 16) & 1023
                        number = w1 & 0xFFFFFF
                        if thread not in expected or number >= fps:
                            raise ValueError("Unexpected VDIF thread or frame number outside supplied rate")
                        epoch = (w1 >> 24) & 63
                        if epoch not in epoch_ticks:
                            origin = f"{2000 + epoch // 2:04d}-{1 + 6 * (epoch % 2):02d}-01T00:00:00Z"
                            epoch_ticks[epoch] = int(utc_ticks(origin, fps))
                        tick = epoch_ticks[epoch] + (w0 & 0x3FFFFFFF) * fps + number
                        invalid = w0 >> 31
                        position = offset + local_offset
                        run = active.get(thread)
                        if run is None or not run.append(tick, position, invalid):
                            if run is not None:
                                run.save(db)
                            active[thread] = Run(shard["name"], thread, tick, position, invalid)
                        frame_count += 1
                    offset += len(chunk)
                if offset != shard["size_bytes"]:
                    raise ValueError(f"VDIF shard changed while indexing: {path}")
            for run in active.values():
                run.save(db)
            shards.append(dict(shard, sha256=digest.hexdigest()))
            db.commit()
        db.execute("CREATE INDEX time_lookup ON runs(thread, start_tick, stop_tick)")
        coverage = []
        for thread in sorted(expected):
            start = stop = None
            missing = count = invalid_count = 0
            for lo, hi, invalid in db.execute(
                    "SELECT start_tick, stop_tick, invalid FROM runs WHERE thread=? ORDER BY start_tick",
                    (thread,)):
                if stop is not None:
                    if lo < stop:
                        raise ValueError(f"Duplicate VDIF frame times in thread {thread}")
                    missing += lo - stop
                else:
                    start = lo
                stop = hi
                count += hi - lo
                invalid_count += (hi - lo) * invalid
            coverage.append(dict(thread=thread, first_tick=start, stop_tick=stop,
                                 frames=count, missing_between_endpoints=missing,
                                 invalid_frames=invalid_count))
        if read_capture_manifest(paths) != capture or any(
                _fingerprint(paths.vdif_dir / name) != fingerprint
                for name, fingerprint in fingerprints.items()):
            raise ValueError("Raw capture changed while indexing")
        db.commit()
        raw_bytes = sum(s["size_bytes"] for s in shards)
        report = dict(
            index_version=INDEX_VERSION, status="complete", capture_block_id=cbid,
            stream_name=stream, raw_directory=str(paths.vdif_dir.resolve()),
            frames_per_second=fps, expected_threads=sorted(expected), geometry=geometry,
            time_axis="TAI seconds since 1970-01-01 TAI, multiplied by frames_per_second",
            reference_epoch_cycle="2000-2031", frames=frame_count, raw_bytes=raw_bytes,
            epoch_origin_ticks=epoch_ticks,
            runs=db.execute("SELECT count(*) FROM runs").fetchone()[0],
            shards=shards, coverage=coverage,
        )
        db.execute("INSERT INTO metadata VALUES ('report', ?)", (json.dumps(report),))
        db.execute("UPDATE metadata SET value='complete' WHERE key='status'")
        db.commit()
    except BaseException:
        db.rollback()
        db.execute("UPDATE metadata SET value='failed' WHERE key='status'")
        db.commit()
        raise
    finally:
        db.close()
    wall = time.perf_counter() - wall_start
    report["benchmark"] = dict(
        wall_seconds=wall, cpu_seconds=time.process_time() - cpu_start,
        input_mib_per_second=raw_bytes / (1024 ** 2) / wall,
        frames_per_second=frame_count / wall,
        process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        index_bytes=(output_dir / "index.sqlite").stat().st_size,
    )
    write_metadata_json(output_dir / "index_report.json", report)
    return report


def export_costs(raw_bytes: int, selected_bytes: int, read_mib_s: float | None = None,
                 write_mib_s: float | None = None) -> dict[str, Any]:
    """Budget one full sequential export read and copies of selected whole frames."""
    if (read_mib_s is None) != (write_mib_s is None):
        raise ValueError("Supply both assumed read and write rates, or neither")
    for rate in (read_mib_s, write_mib_s):
        if rate is not None and (not math.isfinite(rate) or rate <= 0):
            raise ValueError("Assumed I/O rates must be finite and positive")
    result: dict[str, Any] = dict(
        raw_retained_bytes=raw_bytes, additional_vdif_bytes=selected_bytes,
        raw_plus_export_bytes=raw_bytes + selected_bytes,
        index_input_bytes=raw_bytes, export_sequential_read_bytes=raw_bytes,
        export_write_bytes=selected_bytes,
        index_plus_export_io_bytes=2 * raw_bytes + selected_bytes,
        export_cpu_seconds=None,
    )
    if read_mib_s is not None and write_mib_s is not None:
        read_seconds = raw_bytes / (read_mib_s * 1024 ** 2)
        write_seconds = selected_bytes / (write_mib_s * 1024 ** 2)
        result.update(
            assumed_read_mib_per_second=read_mib_s, assumed_write_mib_per_second=write_mib_s,
            serial_io_seconds=read_seconds + write_seconds,
            fully_overlapped_io_seconds=max(read_seconds, write_seconds),
        )
    return result


def plan_export(index_dir: Path, manifest_path: Path, read_mib_s: float | None = None,
                write_mib_s: float | None = None) -> dict[str, Any]:
    """Query a completed index using nominal CSV intervals, without opening raw VDIF.

    Only frames wholly inside [start, end) are selected. No padding or execution
    inference is made. Counts include invalid frames, which are reported apart.
    """
    content = manifest_path.read_bytes()
    manifest = json.loads(content)
    if (not isinstance(manifest, dict) or type(manifest.get("version")) is not int
            or manifest.get("version") != 2
            or manifest.get("timing_authority") != "csv_observation_plan"
            or not isinstance(manifest.get("scans"), list)):
        raise ValueError("A schema-2 CSV observation-plan manifest is required")
    db = sqlite3.connect((index_dir / "index.sqlite").resolve().as_uri() + "?mode=ro", uri=True)
    try:
        status = db.execute("SELECT value FROM metadata WHERE key='status'").fetchone()
        if status != ("complete",):
            raise ValueError("A complete VDIF index is required")
        index = json.loads(db.execute("SELECT value FROM metadata WHERE key='report'").fetchone()[0])
        if index["index_version"] != INDEX_VERSION:
            raise ValueError("Unsupported VDIF index version")
        for key in ("capture_block_id", "stream_name"):
            if key in manifest and manifest[key] != index[key]:
                raise ValueError(f"Scan manifest {key} does not match index")
        fps = index["frames_per_second"]
        windows = []
        names = set()
        for scan in manifest["scans"]:
            if not isinstance(scan, dict) or type(scan.get("include")) is not bool:
                raise ValueError("Every scan needs an explicit boolean include flag")
            if not scan["include"]:
                continue
            name = scan.get("schedule_scan_id")
            if not isinstance(name, str) or not name or name in names:
                raise ValueError("Selected scans need unique schedule_scan_id values")
            names.add(name)
            start, end = utc_ticks(scan["start_time"], fps), utc_ticks(scan["end_time"], fps)
            if end <= start:
                raise ValueError(f"Non-positive scan duration: {name}")
            windows.append((start, end, name, scan))
        windows.sort(key=lambda w: w[0])
        for previous, current in zip(windows, windows[1:]):
            if current[0] < previous[1]:
                raise ValueError("Overlapping selected scan windows require an explicit export policy")
        scans = []
        selected_bytes = 0
        for start, end, name, scan in windows:
            lo, hi = math.ceil(start), max(math.ceil(start), math.floor(end))
            coverage = []
            for thread in index["expected_threads"]:
                count = invalid_count = runs = 0
                for run_start, run_stop, invalid in db.execute(
                        "SELECT start_tick, stop_tick, invalid FROM runs "
                        "WHERE thread=? AND start_tick<? AND stop_tick>?", (thread, hi, lo)):
                    frames = max(0, min(hi, run_stop) - max(lo, run_start))
                    count += frames
                    invalid_count += frames * invalid
                    runs += bool(frames)
                coverage.append(dict(thread=thread, frames=count, missing_frames=hi - lo - count,
                                     invalid_frames=invalid_count, source_runs=runs))
            size = sum(c["frames"] for c in coverage) * index["geometry"]["frame_bytes"]
            selected_bytes += size
            scans.append(dict(
                schedule_scan_id=name, scan_id=scan["scan_id"], start_time=scan["start_time"],
                end_time=scan["end_time"], first_tick=lo, stop_tick=hi, coverage=coverage,
                selected_bytes=size, execution_status="unknown",
                complete_valid_frames=(hi > lo and all(
                    c["missing_frames"] == c["invalid_frames"] == 0 for c in coverage)),
            ))
        return dict(
            version=1, capture_block_id=index["capture_block_id"], stream_name=index["stream_name"],
            scan_manifest_sha256=hashlib.sha256(content).hexdigest(),
            selection_policy="whole frames contained in nominal CSV [start, end); no padding",
            selection_applied_to_vdif=False, raw_revalidated=False,
            frames_per_second=fps, geometry=index["geometry"], scans=scans,
            costs=export_costs(index["raw_bytes"], selected_bytes, read_mib_s, write_mib_s),
        )
    finally:
        db.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build", help="Read and hash a closed capture once")
    build.add_argument("--data-dir", type=Path, required=True)
    build.add_argument("--capture-block-id", required=True)
    build.add_argument("--stream-name", required=True)
    build.add_argument("--output-dir", type=Path, required=True, help="Fresh directory")
    build.add_argument("--frames-per-second", type=int, required=True)
    build.add_argument("--threads", type=int, nargs="+", required=True)
    plan = commands.add_parser("plan", help="Budget exports from an existing index")
    plan.add_argument("--index-dir", type=Path, required=True)
    plan.add_argument("--scan-manifest", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True, help="New JSON file")
    plan.add_argument("--read-mib-per-second", type=float)
    plan.add_argument("--write-mib-per-second", type=float)
    args = parser.parse_args(argv)
    try:
        if args.command == "build":
            result = build_index(args.data_dir, args.capture_block_id, args.stream_name,
                                 args.output_dir, args.frames_per_second, args.threads)
        else:
            result = plan_export(args.index_dir, args.scan_manifest,
                                 args.read_mib_per_second, args.write_mib_per_second)
            with args.output.open("x", encoding="utf-8") as dest:
                json.dump(result, dest, indent=2, sort_keys=True)
                dest.write("\n")
    except (ValueError, KeyError, OSError, sqlite3.Error) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
