#!/usr/bin/env python3
"""Reproducible scan-export benchmark on explicitly selected local storage.

The prepare command copies the supplied real VDIF example into a closed v1
handoff. It never modifies the original. Other commands operate on that copy.
"""

import argparse
from datetime import datetime, timedelta, timezone
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import sys
import time

# scripts/vlbimeta.py is a legacy shell launcher; keep it from shadowing the
# real Python package when this standalone repository tool is invoked directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

CBID = "1"
STREAM = "benchmark"


def dump(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def io_counts():
    return {key.rstrip(":"): int(value) for key, value in
            (line.split() for line in Path("/proc/self/io").read_text().splitlines())}


def evict(paths):
    """Release only this benchmark's clean file pages, never global caches."""
    for path in paths:
        with path.open("rb") as source:
            os.posix_fadvise(source.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)


def prepare(root, source_dir):
    root.mkdir(parents=True, exist_ok=False)
    raw = root / ".vlbi" / CBID / STREAM / "raw"
    raw.mkdir(parents=True)
    start = time.perf_counter()
    inventory = []
    for i, source in enumerate(sorted(source_dir.glob("*.vdif"))):
        dest = raw / f"{CBID}_{STREAM}.{i:08d}"
        with source.open("rb") as src, dest.open("xb") as out:
            shutil.copyfileobj(src, out, 8 * 1024 * 1024)
            out.flush()
            os.fsync(out.fileno())
        inventory.append(dict(name=dest.name, size_bytes=dest.stat().st_size))
        if i % 10 == 0:
            print(f"copied {i + 1} shards", flush=True)
    if not inventory:
        raise ValueError("No VDIF input files")
    capture = dict(version=1, capture_block_id=CBID, stream_name=STREAM,
                   status="closed", shards=inventory)
    dump(raw / "capture.json", capture)
    evict([raw / item["name"] for item in inventory])
    dump(root / "preparation.json", dict(host=socket.getfqdn(), source=str(source_dir),
        data_dir=str(root), raw_bytes=sum(item["size_bytes"] for item in inventory),
        copied_shards=len(inventory), preparation_seconds=time.perf_counter() - start,
        storage=str(root), cache_policy="fsync copy then per-file POSIX_FADV_DONTNEED"))
    print("prepared", root, flush=True)


def make_plan(root):
    """Exercise nine 20-second scans, gaps, a fractional scan and shard crossings."""
    raw = root / ".vlbi" / CBID / STREAM / "raw"
    capture = json.loads((raw / "capture.json").read_text())
    with (raw / capture["shards"][0]["name"]).open("rb") as source:
        words = struct.unpack("<8I", source.read(32))
    epoch = (words[1] >> 24) & 63
    start = datetime(2000 + epoch // 2, 1 + 6 * (epoch % 2), 1, tzinfo=timezone.utc)
    start += timedelta(seconds=words[0] & 0x3fffffff)
    # This source is from 2023, so its epoch interval contains no leap second.
    csv = ["# EXPERIMENT BENCH", "# CAL_PREFIX MCAL", "# PCC_PREFIX MPCC"]
    for i in range(9):
        stamp = start + timedelta(seconds=5 + 30 * i)
        csv.append(f"Bench, azel, 0, 80, scan No{i+1:04d}, "
                   f"{stamp.strftime('%Y-%m-%d %H:%M:%S.%f')}, 20")
    # Fractional boundaries exercise whole-frame rounding in the real export.
    stamp = start + timedelta(seconds=275.0001)
    csv.append(f"Bench, azel, 0, 80, scan No0010, "
               f"{stamp.strftime('%Y-%m-%d %H:%M:%S.%f')}, 3.9998")
    path = root / "tracking.csv"
    path.write_text("\n".join(csv) + "\n")
    from vlbimeta.catalogue import parse_vlbi_catalogue
    from vlbimeta.manifest import build_scan_manifest, manifest_entries_from_catalogue
    manifest = build_scan_manifest(manifest_entries_from_catalogue(parse_vlbi_catalogue(path)), source="csv")
    manifest.update(capture_block_id=CBID, stream_name=STREAM)
    dump(root / "scan_manifest.json", manifest)


def run(root, name):
    from vlbimeta.vdif_index import build_index
    from vlbimeta.vdif_export import export_scans
    raw = root / ".vlbi" / CBID / STREAM / "raw"
    if not (root / "scan_manifest.json").exists():
        make_plan(root)
    paths = sorted(raw.glob(f"{CBID}_{STREAM}.*"))
    evict(paths)
    manifest = root / "scan_manifest.json"
    if name == "export-full":
        # A separate maximum-retention case using the complete contiguous
        # 279-second example. This is a benchmark plan, not an executed schedule.
        import copy
        index = json.loads((root / "index/index_report.json").read_text())
        full = json.loads(manifest.read_text())
        item = copy.deepcopy(full["scans"][0])
        origin = datetime.fromisoformat(item["start_time"].replace("Z", "+00:00")) - timedelta(seconds=5)
        duration = index["raw_bytes"] / (5032 * 4 * 3200)
        item.update(scan_id="scan No0000", schedule_scan_id="No0000",
                    start_time=origin.isoformat(), end_time=(origin + timedelta(seconds=duration)).isoformat())
        full["scans"] = [item]
        manifest = root / "full_scan_manifest.json"
        dump(manifest, full)
    before = io_counts()
    if name == "index":
        result = build_index(root, CBID, STREAM, root / "index", 3200, [0, 1, 2, 3])
    else:
        result = export_scans(root, CBID, STREAM, root / "index", manifest,
                              root / name, experiment="BENCH", station="me")
    after = io_counts()
    dump(root / f"{name}-process-io.json", {key: after[key] - before[key] for key in before})
    print(json.dumps(result.get("benchmark", result), indent=2), flush=True)


def verify(root, name):
    """Independent byte-range oracle for this contiguous four-thread example.

    Use baseband's VDIF decoder and UTC datetime arithmetic, not the exporter's
    TAI tick conversion, to derive source byte ranges. Hash every output byte
    and its independent reference range across original shard boundaries.
    """
    import astropy.units as u
    from baseband.vdif import VDIFHeader
    start = time.perf_counter()
    folder = root / name
    report = json.loads((folder / "export.json").read_text())
    index = json.loads((root / "index" / "index_report.json").read_text())
    raw = root / ".vlbi" / CBID / STREAM / "raw"
    sources = [raw / shard["name"] for shard in index["shards"]]
    fps, nthreads, frame_bytes = 3200, 4, 5032
    assert report["frames_per_second"] == fps
    assert index["expected_threads"] == list(range(nthreads))
    assert all(c["missing_between_endpoints"] == c["invalid_frames"] == 0 for c in index["coverage"])
    assert len({(c["first_tick"], c["stop_tick"], c["frames"]) for c in index["coverage"]}) == 1
    with sources[0].open("rb") as source:
        first = VDIFHeader.fromfile(source)
        assert first["frame_nr"] == first["thread_id"] == 0
        reference = first.get_time(frame_rate=fps * u.Hz).to_datetime(timezone=timezone.utc)
    items = []
    for scan in report["scans"]:
        lo = datetime.fromisoformat(scan["start_time"].replace("Z", "+00:00")) - reference
        hi = datetime.fromisoformat(scan["end_time"].replace("Z", "+00:00")) - reference
        def ticks(delta):
            return Fraction((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds, 1000000) * fps
        import math
        first_tick, stop_tick = math.ceil(ticks(lo)), math.floor(ticks(hi))
        lo_byte, hi_byte = first_tick * nthreads * frame_bytes, stop_tick * nthreads * frame_bytes
        expected = hashlib.sha256()
        cursor = 0
        for path in sources:
            size = path.stat().st_size
            start_byte, end_byte = max(0, lo_byte - cursor), min(size, hi_byte - cursor)
            if end_byte > start_byte:
                with path.open("rb") as source:
                    source.seek(start_byte)
                    remaining = end_byte - start_byte
                    while remaining:
                        block = source.read(min(8 * 1024**2, remaining))
                        if not block:
                            raise ValueError("Truncated reference input")
                        expected.update(block)
                        remaining -= len(block)
            cursor += size
        path = folder / scan["relative_path"]
        actual = hashlib.sha256()
        with path.open("rb") as source:
            while True:
                block = source.read(8 * 1024**2)
                if not block:
                    break
                actual.update(block)
            source.seek(0)
            first_out = VDIFHeader.fromfile(source)
            source.seek(-frame_bytes, os.SEEK_END)
            last_out = VDIFHeader.fromfile(source)
        assert path.stat().st_size == hi_byte - lo_byte == scan["size_bytes"]
        assert expected.hexdigest() == actual.hexdigest() == scan["sha256"]
        assert first_out["thread_id"] == 0 and last_out["thread_id"] == 3
        items.append(dict(scan_id=scan["schedule_scan_id"], bytes=scan["size_bytes"],
            sha256=actual.hexdigest(), first_time=first_out.get_time(frame_rate=fps*u.Hz).isot,
            last_time=last_out.get_time(frame_rate=fps*u.Hz).isot,
            station=first_out.station, threads=[0, 1, 2, 3], byte_equal=True))
    result = dict(status="passed", verifier="baseband VDIF headers + independent UTC byte-range SHA256",
                  scans=items, wall_seconds=time.perf_counter()-start, excluded_from_export_timing=True)
    dump(root / f"{name}-verification.json", result)
    print(json.dumps(result, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "index", "export", "export-repeat", "export-full", "verify"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--export-name", default="export")
    args = parser.parse_args()
    if args.command == "prepare":
        if args.source is None:
            parser.error("--source is required for prepare")
        prepare(args.root, args.source)
        make_plan(args.root)
    elif args.command == "verify":
        for name in (("export", "export-repeat", "export-full") if args.export_name == "all"
                     else (args.export_name,)):
            verify(args.root, name)
    else:
        run(args.root, args.command)


if __name__ == "__main__":
    main()
