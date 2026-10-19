"""Wire-contract tests use both in-memory and optional dedicated real Redis."""

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path

import katsdptelstate
import pytest

from vlbimeta import vex_schedule as vs


FIXTURE = Path(__file__).parent / "fixtures" / "transport.vex"


@pytest.fixture
def schedule():
    # Include CRLF and UTF-8 to catch accidental text-mode normalization.
    content = FIXTURE.read_bytes().replace(b"\n", b"\r\n") + "* café\r\n".encode()
    return vs.VexSchedule("177", "LabContract.vex", content, "Me", "vlbi_online.py", "test-revision")


@pytest.fixture(params=["memory", "redis"])
def telstate(request):
    if request.param == "redis":
        endpoint = os.environ.get("VLBIMETA_TEST_REDIS")
        if not endpoint:
            pytest.skip("Set VLBIMETA_TEST_REDIS to a dedicated disposable Redis endpoint")
        result = katsdptelstate.TelescopeState(endpoint)
    else:
        result = katsdptelstate.TelescopeState()
    # Only these test-owned keys are touched or cleaned up, never FLUSHDB.
    owned = ["vlbi_schedule", "obs_vlbi_schedule", "177_vlbi_schedule",
             "178_vlbi_schedule", "177_obs_vlbi_schedule"]
    if any(key in result for key in owned):
        pytest.fail("VEX contract test keys already exist; use a disposable Redis instance")
    try:
        yield result
    finally:
        for key in owned:
            result.delete(key)


def test_original_bytes_round_trip(telstate, schedule, tmp_path):
    vs.publish_schedule(telstate, schedule)
    # Independent wire assertions catch a producer and consumer sharing a wrong key.
    assert telstate.key_type("177_vlbi_schedule") == katsdptelstate.KeyType.IMMUTABLE
    assert "vlbi_schedule" not in telstate
    assert "177_obs_vlbi_schedule" not in telstate
    wire = telstate["177_vlbi_schedule"]
    assert wire == {
        "version": 1,
        "capture_block_id": "177",
        "vex": {"name": "LabContract.vex", "content": schedule.content,
                "sha256": hashlib.sha256(schedule.content).hexdigest()},
        "station": "Me",
        "producer": {"name": "vlbi_online.py", "revision": "test-revision"},
    }
    consumed = vs.read_schedule(telstate, "177")
    assert consumed == schedule
    vs.write_schedule_snapshot(consumed, tmp_path / "input")
    assert (tmp_path / "input/schedule.vex").read_bytes() == schedule.content
    assert vs.read_schedule_snapshot(tmp_path / "input", "177") == schedule


def test_identical_publication_is_idempotent_but_changes_conflict(telstate, schedule):
    vs.publish_schedule(telstate, schedule)
    vs.publish_schedule(telstate, schedule)
    for change in ({"content": schedule.content + b"* changed\n"}, {"station": "Other"},
                   {"producer_revision": "different"}, {"name": "different.vex"}):
        with pytest.raises(katsdptelstate.ImmutableKeyError):
            vs.publish_schedule(telstate, replace(schedule, **change))
    assert vs.read_schedule(telstate, "177") == schedule


def test_no_root_obs_or_previous_capture_fallback(telstate, schedule):
    # Even a root value that claims the requested identity must be ignored.
    telstate.add("vlbi_schedule", schedule.to_dict(), immutable=True)
    telstate.add("obs_vlbi_schedule", schedule.to_dict(), immutable=True)
    telstate.add("177_obs_vlbi_schedule", schedule.to_dict(), immutable=True)
    vs.publish_schedule(telstate, replace(schedule, capture_block_id="178"))
    with pytest.raises(KeyError, match="Missing 177_vlbi_schedule"):
        vs.read_schedule(telstate.view("178"), "177")
    vs.publish_schedule(telstate.view("178"), schedule)
    assert vs.read_schedule(telstate.view("178"), "177") == schedule
    assert vs.read_schedule(telstate.view("177"), "178").capture_block_id == "178"


def test_mutable_schedule_rejected(telstate, schedule):
    telstate.add("177_vlbi_schedule", schedule.to_dict(), ts=1.0)
    with pytest.raises(ValueError, match="must be immutable"):
        vs.read_schedule(telstate, "177")
    with pytest.raises(katsdptelstate.ImmutableKeyError):
        vs.publish_schedule(telstate, schedule)


def test_wrong_capture_in_correct_namespace_rejected(telstate, schedule):
    telstate.add("178_vlbi_schedule", schedule.to_dict(), immutable=True)
    with pytest.raises(ValueError, match="does not match"):
        vs.read_schedule(telstate, "178")


@pytest.mark.parametrize("change", [
    "missing_digest", "bad_digest", "uppercase_digest", "text_content", "empty_content",
    "unknown_field", "missing_station", "empty_station", "version", "bool_version",
    "missing_revision", "path_name", "wrong_capture", "list_bundle",
])
def test_invalid_wire_value_rejected(schedule, change):
    wire = schedule.to_dict()
    if change == "missing_digest":
        del wire["vex"]["sha256"]
    elif change == "bad_digest":
        wire["vex"]["sha256"] = "0" * 64
    elif change == "uppercase_digest":
        wire["vex"]["sha256"] = wire["vex"]["sha256"].upper()
    elif change == "text_content":
        wire["vex"]["content"] = wire["vex"]["content"].decode()
    elif change == "empty_content":
        wire["vex"]["content"] = b""
    elif change == "unknown_field":
        wire["experiment"] = "guess"
    elif change == "missing_station":
        del wire["station"]
    elif change == "empty_station":
        wire["station"] = " "
    elif change == "version":
        wire["version"] = 2
    elif change == "bool_version":
        wire["version"] = True
    elif change == "missing_revision":
        del wire["producer"]["revision"]
    elif change == "path_name":
        wire["vex"]["name"] = "../LabContract.vex"
    elif change == "wrong_capture":
        wire["capture_block_id"] = "178"
    elif change == "list_bundle":
        wire = []
    with pytest.raises(ValueError):
        vs.VexSchedule.from_dict(wire, capture_block_id="177")


@pytest.mark.parametrize("capture", ["", "177\n", "177_other", "../177", "١٧٧", 177, "1" * 129])
def test_invalid_capture_rejected(schedule, capture):
    with pytest.raises(ValueError, match="capture_block_id"):
        replace(schedule, capture_block_id=capture)
    with pytest.raises(ValueError, match="capture_block_id"):
        vs.read_schedule(katsdptelstate.TelescopeState(), capture)


def test_read_file_is_bounded_and_binary(tmp_path, monkeypatch):
    path = tmp_path / "input.vex"
    path.write_bytes(b"abc\r\n")
    assert vs.read_vex_file(path) == b"abc\r\n"
    monkeypatch.setattr(vs, "MAX_VEX_BYTES", 4)
    with pytest.raises(ValueError, match="original bytes"):
        vs.read_vex_file(path)
    path.write_bytes(b"")
    with pytest.raises(ValueError, match="original bytes"):
        vs.read_vex_file(path)


def test_snapshot_survives_loss_of_live_telstate(telstate, schedule, tmp_path):
    vs.publish_schedule(telstate, schedule)
    vs.write_schedule_snapshot(vs.read_schedule(telstate, "177"), tmp_path / "input")
    telstate.delete("177_vlbi_schedule")
    assert vs.read_schedule_snapshot(tmp_path / "input", "177") == schedule


def test_snapshot_refuses_existing_destination(schedule, tmp_path):
    output = tmp_path / "input"
    vs.write_schedule_snapshot(schedule, output)
    before = {p.name: p.read_bytes() for p in output.iterdir()}
    with pytest.raises(FileExistsError):
        vs.write_schedule_snapshot(replace(schedule, station="Other"), output)
    assert {p.name: p.read_bytes() for p in output.iterdir()} == before


@pytest.mark.parametrize("change", ["content", "missing_metadata", "bad_size", "path", "identity", "symlink"])
def test_snapshot_tampering_or_partial_write_rejected(schedule, tmp_path, change):
    output = tmp_path / "input"
    vs.write_schedule_snapshot(schedule, output)
    metadata = output / "schedule.json"
    if change == "content":
        path = output / "schedule.vex"
        path.write_bytes(b"X" + path.read_bytes()[1:])  # same size, wrong hash
    elif change == "missing_metadata":
        metadata.unlink()
    elif change == "symlink":
        original = metadata.read_bytes()
        metadata.unlink()
        (tmp_path / "outside.json").write_bytes(original)
        metadata.symlink_to(tmp_path / "outside.json")
    else:
        value = json.loads(metadata.read_text())
        if change == "bad_size":
            value["vex"]["size_bytes"] += 1
        elif change == "path":
            value["vex"]["content_file"] = "../outside.vex"
        elif change == "identity":
            value["capture_block_id"] = "178"
        metadata.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        vs.read_schedule_snapshot(output, "177")


def test_verify_cli_is_offline_and_does_not_print_content(schedule, tmp_path, capsys):
    output = tmp_path / "input"
    vs.write_schedule_snapshot(schedule, output)
    assert vs.main(["verify", "--capture-block-id", "177", "--input-dir", str(output)]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed == schedule.snapshot_metadata()
    assert "content" not in printed["vex"]
    with pytest.raises(SystemExit) as error:
        vs.main(["verify", "--capture-block-id", "178", "--input-dir", str(output)])
    assert error.value.code == 1
    assert "does not match" in capsys.readouterr().err


def test_publication_cli_and_snapshot(telstate, monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(vs.katsdptelstate, "TelescopeState", lambda endpoint: telstate)
    assert vs.main(["publish", "--telstate", "fixture", "--capture-block-id", "177",
                    "--vex", str(FIXTURE), "--station", "Me", "--producer", "lab",
                    "--producer-revision", "r1"]) == 0
    assert vs.main(["snapshot", "--telstate", "fixture", "--capture-block-id", "177",
                    "--output-dir", str(tmp_path / "input")]) == 0
    assert vs.read_schedule_snapshot(tmp_path / "input", "177").content == FIXTURE.read_bytes()
    capsys.readouterr()
