"""Independent packet/byte oracles for indexing and whole-frame export budgets."""

import hashlib
import json
import os
import sqlite3
import struct

import pytest

from vlbimeta.vdif_index import build_index, export_costs, main, plan_export, utc_ticks


def frame(number, thread=0, *, seconds=0, epoch=53, invalid=False):
    # 32-byte standard header + identifiable 8-byte payload; four frames/second.
    return struct.pack("<8I", seconds | (int(invalid) << 31), (epoch << 24) | number,
                       (1 << 29) | 5, (1 << 26) | (thread << 16) | 28005, 0, 0, 0, 0) + bytes([
                           thread, number, seconds % 256, epoch, 4, 5, 6, 7])


def capture(tmp_path, shards):
    raw = tmp_path / ".vlbi/1/test/raw"
    raw.mkdir(parents=True)
    inventory = []
    for i, content in enumerate(shards):
        name = f"1_test.{i:08d}"
        (raw / name).write_bytes(content)
        inventory.append(dict(name=name, size_bytes=len(content)))
    (raw / "capture.json").write_text(json.dumps(dict(
        version=1, capture_block_id="1", stream_name="test", status="closed", shards=inventory)))
    return raw


def build(tmp_path, threads=(0, 1)):
    return build_index(tmp_path, "1", "test", tmp_path / "index", 4, threads)


def scan(start="00:00:00", end="00:00:01", name="No0001", include=True):
    return dict(scan_id="scan " + name, schedule_scan_id=name, include=include,
                start_time="2026-07-01T" + start + "Z", end_time="2026-07-01T" + end + "Z")


def plan(tmp_path, scans, **extra):
    manifest = dict(version=2, timing_authority="csv_observation_plan", scans=scans, **extra)
    path = tmp_path / "scan_manifest.json"
    path.write_text(json.dumps(manifest))
    return plan_export(tmp_path / "index", path, 100, 50)


def test_interleaving_shards_and_byte_offsets(tmp_path):
    packets = [frame(n, t) for n in range(4) for t in range(2)]
    shards = [b"".join(packets[:5]), b"".join(packets[5:])]
    raw = capture(tmp_path, shards)
    report = build(tmp_path)
    assert report["frames"] == 8
    assert report["runs"] == 4
    assert report["raw_bytes"] == 320
    assert report["geometry"]["station_id"] == 28005
    assert [s["sha256"] for s in report["shards"]] == [hashlib.sha256(s).hexdigest() for s in shards]
    assert all(c["missing_between_endpoints"] == c["invalid_frames"] == 0 for c in report["coverage"])
    origin = int(utc_ticks("2026-07-01T00:00:00Z", 4))
    recovered = {}
    with sqlite3.connect(tmp_path / "index/index.sqlite") as db:
        for name, thread, lo, hi, offset, stride, invalid in db.execute("SELECT * FROM runs"):
            assert invalid == 0
            with (raw / name).open("rb") as source:
                for i in range(hi - lo):
                    source.seek(offset + i * stride)
                    recovered[(lo + i - origin, thread)] = source.read(40)
    assert recovered == {(n, t): frame(n, t) for n in range(4) for t in range(2)}
    result = plan(tmp_path, [scan(), scan(name="local", include=False)])
    assert result["scans"][0]["selected_bytes"] == 320
    assert result["scans"][0]["complete_valid_frames"]
    assert result["costs"]["raw_plus_export_bytes"] == 640
    assert result["costs"]["index_plus_export_io_bytes"] == 960
    assert not result["selection_applied_to_vdif"]
    assert not result["raw_revalidated"]
    assert result["costs"]["export_cpu_seconds"] is None
    assert [p.read_bytes() for p in sorted(raw.glob("1_test.*"))] == shards


def test_disorder_gaps_invalid_and_missing_thread(tmp_path):
    capture(tmp_path, [frame(3) + frame(0) + frame(1, invalid=True)])
    report = build(tmp_path)
    assert report["coverage"][0]["missing_between_endpoints"] == 1
    assert report["coverage"][0]["invalid_frames"] == 1
    assert report["coverage"][1]["frames"] == 0
    result = plan(tmp_path, [scan()])["scans"][0]
    assert not result["complete_valid_frames"]
    assert result["coverage"][0]["missing_frames"] == 1
    assert result["coverage"][1]["missing_frames"] == 4
    assert result["selected_bytes"] == 120  # Invalid packets retained in byte budget.


def test_fractional_adjacent_empty_and_outside_windows(tmp_path):
    capture(tmp_path, [b"".join(frame(n) for n in range(4))])
    build(tmp_path, (0,))
    result = plan(tmp_path, [
        scan(end="00:00:00.500000"),
        scan(start="00:00:00.500001", end="00:00:01", name="No0002"),
        scan(start="00:00:01", end="00:00:01.100000", name="No0003"),
        scan(start="00:00:02", end="00:00:03", name="No0004"),
    ])
    assert [s["selected_bytes"] for s in result["scans"]] == [80, 40, 0, 0]
    assert [s["complete_valid_frames"] for s in result["scans"]] == [True, True, False, False]
    assert result["scans"][3]["coverage"][0]["missing_frames"] == 4


def test_leap_second_and_epoch_change(tmp_path):
    # July 2016 epoch, the inserted second, then January 2017 epoch.
    capture(tmp_path, [frame(3, epoch=33, seconds=184 * 86400)
                      + frame(0, epoch=34, seconds=0)])
    report = build(tmp_path, (0,))
    assert report["runs"] == 1
    assert report["coverage"][0]["missing_between_endpoints"] == 0
    jan = utc_ticks("2017-01-01T00:00:00Z", 4)
    dec = utc_ticks("2016-12-31T23:59:59Z", 4)
    assert jan - dec == 8  # Two physical seconds, including 23:59:60.
    assert report["coverage"][0]["stop_tick"] == jan + 1


@pytest.mark.parametrize("shards", [
    [frame(0) + frame(0)], [frame(0), frame(0)],
    [frame(0, epoch=33, seconds=184 * 86400 + 1), frame(0, epoch=34)],
])
def test_duplicates_fail_even_between_shards_or_epochs(tmp_path, shards):
    capture(tmp_path, shards)
    with pytest.raises(ValueError, match="Duplicate VDIF"):
        build(tmp_path, (0,))
    with sqlite3.connect(tmp_path / "index/index.sqlite") as db:
        assert db.execute("SELECT value FROM metadata WHERE key='status'").fetchone() == ("failed",)
    with pytest.raises(ValueError, match="complete VDIF index"):
        plan(tmp_path, [scan()])
    with pytest.raises(FileExistsError):
        build(tmp_path, (0,))


@pytest.mark.parametrize("word,mask", [(0, 1 << 30), (1, 1 << 30), (2, 1 << 30), (4, 1 << 24)])
def test_unsupported_headers(tmp_path, word, mask):
    packet = bytearray(frame(0))
    value = struct.unpack_from("<I", packet, 4 * word)[0]
    struct.pack_into("<I", packet, 4 * word, value | mask)
    capture(tmp_path, [packet])
    with pytest.raises(ValueError, match="Unsupported VDIF"):
        build(tmp_path)


@pytest.mark.parametrize("bad", [frame(0)[:20], frame(0)[:-1], frame(0) + frame(1)[:-1]])
def test_truncation(tmp_path, bad):
    capture(tmp_path, [bad])
    with pytest.raises(ValueError, match="[Tt]runcated"):
        build(tmp_path)


@pytest.mark.parametrize("bad", [frame(4), frame(0, 2)])
def test_instrument_contract(tmp_path, bad):
    capture(tmp_path, [bad])
    with pytest.raises(ValueError, match="Unexpected VDIF thread or frame number"):
        build(tmp_path)


def test_changed_station(tmp_path):
    changed = bytearray(frame(1))
    changed[12] ^= 1
    capture(tmp_path, [frame(0) + changed])
    with pytest.raises(ValueError, match="geometry changed"):
        build(tmp_path)


def test_compression_and_chunk_boundaries(tmp_path, monkeypatch):
    # Many input frames, but just one affine run per thread.
    monkeypatch.setattr("vlbimeta.vdif_index.CHUNK_BYTES", 400)
    capture(tmp_path, [b"".join(frame(n, t, seconds=s)
                              for s in range(100) for n in range(4) for t in (0, 1))])
    report = build(tmp_path)
    assert report["frames"] == 800
    assert report["runs"] == 2


@pytest.mark.parametrize("scans,extra,match", [
    ([scan(), scan(start="00:00:00.5", name="No0002")], {}, "Overlapping"),
    ([scan(), scan()], {}, "unique"),
    ([scan(include="true")], {}, "boolean"),
    ([scan(end="00:00:00")], {}, "Non-positive"),
    ([scan()], {"capture_block_id": "2"}, "does not match"),
])
def test_bad_plans(tmp_path, scans, extra, match):
    capture(tmp_path, [frame(0)])
    build(tmp_path, (0,))
    with pytest.raises(ValueError, match=match):
        plan(tmp_path, scans, **extra)


def test_naive_times_rejected():
    with pytest.raises(ValueError, match="UTC offset"):
        utc_ticks("2026-07-01T00:00:00", 4)


def test_output_must_not_modify_raw(tmp_path):
    raw = capture(tmp_path, [frame(0)])
    with pytest.raises(ValueError, match="outside the raw"):
        build_index(tmp_path, "1", "test", raw / "index", 4, (0,))
    assert not (raw / "index").exists()


def test_raw_change_during_indexing(tmp_path, monkeypatch):
    from vlbimeta import vdif_index

    raw = capture(tmp_path, [frame(0)])
    source = raw / "1_test.00000000"
    original = vdif_index._geometry

    def change_after_initial_stat(words):
        stat = source.stat()
        data = bytearray(source.read_bytes())
        data[-1] ^= 1  # Same-sized change to payload during the index pass.
        source.write_bytes(data)
        os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1000000000))
        monkeypatch.setattr(vdif_index, "_geometry", original)
        return original(words)

    monkeypatch.setattr(vdif_index, "_geometry", change_after_initial_stat)
    with pytest.raises(ValueError, match="Raw capture changed"):
        build(tmp_path, (0,))
    assert not (tmp_path / "index/index_report.json").exists()


def test_costs_units_and_assumptions():
    costs = export_costs(100 * 1024 ** 2, 50 * 1024 ** 2, 10, 5)
    assert costs["serial_io_seconds"] == 20
    assert costs["fully_overlapped_io_seconds"] == 10
    assert "serial_io_seconds" not in export_costs(100, 50)
    for bad in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="finite and positive"):
            export_costs(100, 50, bad, 5)
    with pytest.raises(ValueError, match="both assumed"):
        export_costs(100, 50, 10)


def test_cli_and_existing_outputs(tmp_path, capsys):
    capture(tmp_path, [frame(0)])
    args = ["build", "--data-dir", str(tmp_path), "--capture-block-id", "1", "--stream-name", "test",
            "--output-dir", str(tmp_path / "index"), "--frames-per-second", "4", "--threads", "0"]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["frames"] == 1
    with pytest.raises(SystemExit) as exc:
        main(args)
    assert exc.value.code == 1
    plan(tmp_path, [scan()])
    destination = tmp_path / "plan.json"
    plan_args = ["plan", "--index-dir", str(tmp_path / "index"),
                 "--scan-manifest", str(tmp_path / "scan_manifest.json"), "--output", str(destination)]
    assert main(plan_args) == 0
    assert json.loads(destination.read_text())["scans"][0]["selected_bytes"] == 40
    previous = destination.read_bytes()
    with pytest.raises(SystemExit) as exc:
        main(plan_args)
    assert exc.value.code == 1
    assert destination.read_bytes() == previous
