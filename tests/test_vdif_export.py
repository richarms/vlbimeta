"""Independent explicit packet oracles for the diagnostic physical exporter."""

import hashlib
import json

import pytest

from vlbimeta.vdif_export import export_scans
from vlbimeta.vdif_index import build_index
from .test_vdif_index import capture, frame, scan


def prepare_capture(tmp_path, packets, scans=None, threads=(0, 1), split=5):
    raw = capture(tmp_path, [b"".join(packets[:split]), b"".join(packets[split:])])
    build_index(tmp_path, "1", "test", tmp_path / "index", 4, threads)
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(dict(version=2, timing_authority="csv_observation_plan",
        capture_block_id="1", stream_name="test", scans=scans or [scan()])))
    return raw, path


def export(tmp_path, path, **kwargs):
    return export_scans(tmp_path, "1", "test", tmp_path / "index", path,
                        tmp_path / "export", experiment="test", station="me", **kwargs)


def test_exact_bytes_shard_crossing_and_fractional_boundaries(tmp_path):
    packets = [frame(n, t) for n in range(4) for t in (0, 1)]
    raw, path = prepare_capture(tmp_path, packets, [
        scan(end="00:00:00.500000"),
        scan(start="00:00:00.500001", end="00:00:01", name="No0002"),
        scan(name="notselected", include=False),
    ])
    original = {p.name: p.read_bytes() for p in raw.iterdir()}
    report = export(tmp_path, path)
    expected = [b"".join(packets[:4]), b"".join(packets[6:8])]
    assert [s["size_bytes"] for s in report["scans"]] == [160, 80]
    for entry, content in zip(report["scans"], expected):
        assert (tmp_path / "export" / entry["relative_path"]).read_bytes() == content
        assert entry["sha256"] == hashlib.sha256(content).hexdigest()
    assert report["exported_bytes"] == 240
    assert report["raw_revalidated"]
    assert report["selection_applied_to_vdif"]
    assert not report["science_validated"]
    assert {p.name: p.read_bytes() for p in raw.iterdir()} == original
    assert not (tmp_path / "export.writing").exists()
    with pytest.raises(FileExistsError):
        export(tmp_path, path)


def test_export_handles_chunk_boundaries(tmp_path, monkeypatch):
    packets = [frame(n, t, seconds=s) for s in range(2) for n in range(4) for t in (0, 1)]
    _, path = prepare_capture(tmp_path, packets, [scan(), scan(start="00:00:01", end="00:00:02", name="No0002")])
    monkeypatch.setattr("vlbimeta.vdif_export.CHUNK_BYTES", 120)
    report = export(tmp_path, path)
    assert [(tmp_path / "export" / s["relative_path"]).read_bytes() for s in report["scans"]] == [
        b"".join(packets[:8]), b"".join(packets[8:])]


def test_changed_payload_refuses_publication(tmp_path):
    packets = [frame(n, t) for n in range(4) for t in (0, 1)]
    raw, path = prepare_capture(tmp_path, packets)
    shard = sorted(raw.glob("1_test.*"))[0]
    content = bytearray(shard.read_bytes())
    content[-1] ^= 1
    shard.write_bytes(content)
    with pytest.raises(ValueError, match="SHA256"):
        export(tmp_path, path)
    assert not (tmp_path / "export").exists()
    assert (tmp_path / "export.writing").exists()


def test_disordered_frames_rejected_without_publication(tmp_path):
    packets = [frame(3), frame(0), frame(1), frame(2)]
    _, path = prepare_capture(tmp_path, packets, threads=(0,), split=2)
    with pytest.raises(ValueError, match="time/thread-ordered"):
        export(tmp_path, path)
    assert not (tmp_path / "export").exists()


def test_partial_and_invalid_frames_require_opt_in(tmp_path):
    packets = [frame(0), frame(1, invalid=True), frame(3)]
    _, path = prepare_capture(tmp_path, packets, threads=(0,), split=1)
    with pytest.raises(ValueError, match="partial export"):
        export(tmp_path, path)
    report = export(tmp_path, path, allow_partial=True)
    assert (tmp_path / "export" / report["scans"][0]["relative_path"]).read_bytes() == b"".join(packets)
    assert not report["scans"][0]["complete_valid_frames"]


def test_no_path_traversal(tmp_path):
    packets = [frame(n) for n in range(4)]
    _, path = prepare_capture(tmp_path, packets, [scan(name="../escape")], threads=(0,), split=2)
    with pytest.raises(ValueError, match="alphanumeric"):
        export(tmp_path, path)


def test_empty_scan_explicit_with_partial_opt_in(tmp_path):
    packets = [frame(n) for n in range(4)]
    _, path = prepare_capture(tmp_path, packets, [scan(start="00:00:02", end="00:00:03")], threads=(0,), split=2)
    report = export(tmp_path, path, allow_partial=True)
    assert report["scans"][0]["relative_path"] is None
    assert report["exported_bytes"] == 0
