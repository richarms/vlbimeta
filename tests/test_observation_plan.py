"""Canonical CSV input, real observing-plan fixtures and producer compatibility."""

import ast
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import katsdptelstate
import pytest

from vlbimeta import controller_entrypoint, observation_plan
from vlbimeta.catalogue import parse_vlbi_catalogue
from vlbimeta.manifest import build_scan_manifest, manifest_entries_from_catalogue
from vlbimeta.runtime import materialise_catalogue_from_telstate


FIXTURES = Path(__file__).parent / "fixtures" / "observing_plans"
CSV = b"""# EXPERIMENT Lab123
# CAL_PREFIX MCAL
# PCC_PREFIX MPCC
target, radec bpcal, 12:00:00, -30:00:00, scan No0004, 2026-09-11 12:00:00.125, 1.25
target, radec, 12:00:00, -30:00:00, mcal No0000, 2026-09-11 12:00:02.000, 1
"""


def wire(content=CSV):
    return {
        "vlbi_catalogue_content": content,
        "vlbi_catalogue_name": "observation.csv",
        "vlbi_catalogue_sha256": hashlib.sha256(content).hexdigest(),
        "vlbi_catalogue_format": "csv",
    }


@pytest.mark.parametrize("experiment,rows,scheduled", [
    ("eg144", 84, 81), ("el080", 371, 366), ("es123a", 56, 54),
    ("ex011a", 111, 108), ("gs047c", 42, 39), ("n26l2", 16, 14),
])
def test_real_csv_plans(experiment, rows, scheduled, tmp_path):
    path = FIXTURES / f"vlbi_cat_{experiment}.csv"
    assert observation_plan.main(["--catalogue", str(path), "--output-dir", str(tmp_path / "plan")]) == 0
    output = tmp_path / "plan"
    assert (output / "catalogue" / path.name).read_bytes() == path.read_bytes()
    manifest = json.loads((output / "scan_manifest.json").read_text())
    assert manifest["experiment"].casefold() == experiment
    assert len(manifest["scans"]) == rows
    included = [s for s in manifest["scans"] if s["include"]]
    assert len(included) == scheduled
    assert all(s["schedule_scan_id"] == s["scan_id"][5:] for s in included)
    assert manifest["selection_applied_to_vdif"] is False
    assert manifest["timing_authority"] == "csv_observation_plan"
    assert manifest["catalogue"]["catalogue_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert all(s["execution_status"] == "unknown" for s in manifest["scans"])


def test_csv_timing_is_preserved_instead_of_vex_station_offset():
    catalogue = parse_vlbi_catalogue(FIXTURES / "vlbi_cat_es123a.csv")
    scan = next(s for s in manifest_entries_from_catalogue(catalogue) if s.scan_id == "scan No0004")
    # Matching VEX Me interval is start+14 .. start+210; this CSV chooses start .. start+210.
    assert scan.start_time.isoformat() == "2026-06-05T23:11:00+00:00"
    assert (scan.end_time - scan.start_time).total_seconds() == 210


def test_fractional_intervals_and_network_calibrators_are_preserved(tmp_path):
    path = tmp_path / "input.csv"
    path.write_bytes(CSV)
    manifest = build_scan_manifest(manifest_entries_from_catalogue(parse_vlbi_catalogue(path)), source="csv")
    scan, local = manifest["scans"]
    assert scan["start_time"] == "2026-09-11T12:00:00.125000Z"
    assert scan["end_time"] == "2026-09-11T12:00:01.375000Z"
    assert scan["include"] is True  # bpcal is a network scan, not a local MCAL operation
    assert scan["schedule_scan_id"] == "No0004"
    assert local["tags"] == ["local_calibration"]
    assert local["schedule_scan_id"] is None
    assert local["include"] is False


@pytest.mark.parametrize("content", [
    CSV.replace(b"# EXPERIMENT Lab123\n", b""),
    CSV + CSV.splitlines(keepends=True)[3],
    CSV.replace(b"1.25\n", b"nan\n"),
    CSV.replace(b"1.25\n", b"inf\n"),
    CSV.replace(b"1.25\n", b"0\n"),
    CSV.replace(b"1.25\n", b"-1\n"),
    CSV + b"# EXPERIMENT Other\n",
    b"# EXPERIMENT Empty\n",
])
def test_invalid_plan_rejected(content, tmp_path):
    path = tmp_path / "input.csv"
    path.write_bytes(content)
    with pytest.raises(ValueError):
        parse_vlbi_catalogue(path)


@pytest.mark.parametrize("text_content", [False, True])
def test_exact_csv_bytes_and_exclusive_capture_view(tmp_path, text_content):
    telstate = katsdptelstate.TelescopeState()
    content = CSV.replace(b"\n", b"\r\n")
    values = wire(content)
    if text_content:
        values["vlbi_catalogue_content"] = content.decode()
    for key, value in values.items():
        telstate.add(key, value, immutable=True)
        telstate.view("178").add(key, value, immutable=True)
        telstate.view("177").add("obs_" + key, value, immutable=True)
    with pytest.raises(KeyError, match="177_vlbi_catalogue_content"):
        materialise_catalogue_from_telstate(telstate.view("178"), tmp_path / "absent", capture_block_id="177")
    assert not (tmp_path / "absent").exists()
    for key, value in values.items():
        telstate.view("177").add(key, value, immutable=True)
    path, _ = materialise_catalogue_from_telstate(telstate.view("178"), tmp_path / "input", capture_block_id="177")
    assert path.read_bytes() == content


@pytest.mark.parametrize("change", ["missing", "digest", "format", "name", "mutable"])
def test_invalid_transport_fails_before_materialisation(tmp_path, change):
    values = wire()
    if change == "missing":
        del values["vlbi_catalogue_sha256"]
    elif change == "digest":
        values["vlbi_catalogue_sha256"] = "0" * 64
    elif change == "format":
        values["vlbi_catalogue_format"] = "yaml"
    elif change == "name":
        values["vlbi_catalogue_name"] = "../escape.csv"
    telstate = katsdptelstate.TelescopeState()
    for key, value in values.items():
        telstate.view("177").add(key, value, immutable=change != "mutable")
    with pytest.raises((KeyError, ValueError)):
        materialise_catalogue_from_telstate(telstate, tmp_path / "input", capture_block_id="177")
    assert not (tmp_path / "input").exists()


def test_antab_requires_csv_even_when_packaged_file_exists(tmp_path, monkeypatch):
    # Validate default input behavior without running numerical ANTAB generation.
    from .test_runtime import make_capture

    paths, _ = make_capture(tmp_path)
    monkeypatch.setattr(controller_entrypoint, "_derive_obs_params", lambda *args: {"proposal_id": "ES116A"})
    monkeypatch.setattr(controller_entrypoint, "_open_capture_telstate", lambda *args: katsdptelstate.TelescopeState())
    with pytest.raises(KeyError, match="Missing capture-scoped CSV"):
        controller_entrypoint.main([str(tmp_path), "177", "sdp_vdif", "--telstate", "unused"])
    assert not paths.final_dir.exists()
    assert not paths.final_vdif_dir.exists()


def test_experiment_override_cannot_replace_csv_identity(tmp_path, monkeypatch):
    from .test_runtime import make_capture

    paths, _ = make_capture(tmp_path)
    monkeypatch.setattr(controller_entrypoint, "_derive_obs_params", lambda *args: {})
    monkeypatch.setattr(controller_entrypoint, "_open_capture_telstate", lambda *args: wire())
    with pytest.raises(ValueError, match="conflicts with the canonical CSV"):
        controller_entrypoint.main([str(tmp_path), "177", "sdp_vdif", "--telstate", "unused",
                                   "--experiment", "Other"])
    assert not paths.final_dir.exists()
    assert not paths.final_vdif_dir.exists()


@pytest.fixture
def observation_publisher():
    default = Path(__file__).parents[2] / "mgouws_vlbi/vlbi/observation/vlbi_online.py"
    path = Path(os.environ.get("VLBI_OBSERVATION_SCRIPT", default))
    if not path.exists():
        pytest.skip("Set VLBI_OBSERVATION_SCRIPT to check the actual observation publisher")
    # Load only the two helpers: never execute telescope control code on import.
    tree = ast.parse(path.read_text())
    names = {"_get_capture_telstate", "persist_vlbi_catalogue"}
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(nodes) == 2
    namespace = {"Path": Path, "hashlib": hashlib}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    return namespace["persist_vlbi_catalogue"]


@pytest.fixture(params=["memory", "redis"])
def producer_telstate(request):
    endpoint = ""
    if request.param == "redis":
        endpoint = os.environ.get("VLBIMETA_TEST_REDIS")
        if not endpoint:
            pytest.skip("Set VLBIMETA_TEST_REDIS to a disposable Redis endpoint")
    telstate = katsdptelstate.TelescopeState(endpoint)
    keys = ["177_" + key for key in wire()] + ["177_vlbi_experiment"]
    if any(key in telstate for key in keys):
        pytest.fail("Use a disposable Redis without existing CSV test keys")
    try:
        yield telstate
    finally:
        for key in keys:
            telstate.delete(key)


def test_actual_observation_publisher_uses_frozen_input(observation_publisher, producer_telstate, tmp_path):
    telstate = producer_telstate
    session = SimpleNamespace(telstate=telstate, capture_block_id="177")
    path = tmp_path / "observation.csv"
    path.write_bytes(b"changed after the observation script read it")
    observation_publisher(session, path, "Lab123", content=CSV)
    observation_publisher(session, path, "Lab123", content=CSV)  # identical retry
    saved, _ = materialise_catalogue_from_telstate(telstate, tmp_path / "input", capture_block_id="177")
    assert saved.read_bytes() == CSV
    assert "vlbi_catalogue_content" not in telstate
    assert "177_obs_vlbi_catalogue_content" not in telstate
    with pytest.raises(katsdptelstate.ImmutableKeyError):
        observation_publisher(session, path, "Lab123", content=CSV + b"\n")


def test_observation_publisher_requires_capture_id(observation_publisher, tmp_path):
    telstate = katsdptelstate.TelescopeState()
    session = SimpleNamespace(telstate=telstate)
    with pytest.raises(KeyError):
        observation_publisher(session, tmp_path / "input.csv", "Lab123", content=CSV)
    assert not telstate.keys()
