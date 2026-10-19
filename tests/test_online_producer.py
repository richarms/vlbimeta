"""Frozen CSV parsing and optional comparison with the katsdpscripts observer."""

import ast
import io
import os
from pathlib import Path

import katpoint
import pytest

from vlbimeta.catalogue import parse_vlbi_catalogue_content


FIXTURES = Path(__file__).parent / "fixtures" / "observing_plans"


def test_frozen_bytes_do_not_reopen_source(tmp_path):
    path = tmp_path / "plan.csv"
    content = (b"# EXPERIMENT FROZEN\r\n"
               b"Target, azel, 0, 80, scan No0001, 2026-09-24 12:00:00.125, 1.25\r\n")
    path.write_bytes(b"changed after the observer read the file")
    parsed = parse_vlbi_catalogue_content(content, source=str(path))
    assert parsed.obs_params["EXPERIMENT"] == "FROZEN"
    assert parsed.scans[0].start_time.microsecond == 125000
    assert parsed.scans[0].duration == 1.25


@pytest.fixture
def observation_parser():
    default = Path(__file__).parents[2] / "katsdpscripts/observation/vlbi_online.py"
    path = Path(os.environ.get("VLBI_OBSERVATION_SCRIPT", default))
    if not path.exists():
        pytest.skip("Set VLBI_OBSERVATION_SCRIPT to compare the katsdpscripts CSV parser")
    # Load only the parser; importing the whole script would control the telescope.
    nodes = [n for n in ast.parse(path.read_text()).body
             if isinstance(n, ast.FunctionDef) and n.name == "parse_vlbi_cat"]
    assert len(nodes) == 1
    namespace = {"io": io, "katpoint": katpoint}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    return namespace["parse_vlbi_cat"]


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*.csv")), ids=lambda path: path.stem)
def test_site_csv_interpretation_matches(path, observation_parser):
    content = path.read_bytes()
    # A nonexistent pathname ensures both parsers use the supplied frozen bytes.
    online_params, online_scans, _ = observation_parser("not-read.csv", content=content)
    offline = parse_vlbi_catalogue_content(content)
    assert online_params["EXPERIMENT"] == offline.obs_params["EXPERIMENT"]
    assert len(online_scans) == len(offline.scans)
    for observed, processed in zip(online_scans, offline.scans):
        assert (observed["name"], observed["start_ut"], observed["duration"]) == (
            processed.name, processed.start_ts, processed.duration)
        assert observed["kp_tgt"] == processed.kp_target
