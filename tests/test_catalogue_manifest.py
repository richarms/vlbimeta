from datetime import datetime, timezone

import pytest

pytest.importorskip("katpoint")

from vlbimeta.catalogue import legacy_scan_dict, parse_vlbi_catalogue
from vlbimeta.manifest import build_scan_manifest, manifest_entries_from_catalogue, scan_class


CATALOGUE_TEXT = """# EXPERIMENT EP134F
# CH01 1626.49 LSB 32.00 RCP
# CH02 1626.49 LSB 32.00 LCP
# CH03 1626.49 USB 32.00 RCP
# CH04 1626.49 USB 32.00 LCP
# POL XY
# CAL_PREFIX scan cal
# PCC_PREFIX mpcc
J0408-6545, radec bfcal, 04:08:20.37884, -65:45:09.0806, scan cal00, 2026-04-22 12:13:00.000, 240
*J1029A, radec, 10:29:13.9500000, 26:23:17.960000, scan No0001, 2026-04-22 12:19:00.000, 60
J1939-6342, radec, 19:39:25.03, -63:42:45.7, mpcc No0001, 2026-04-22 12:21:00.000, 120
notrack, radec, 00:00:00.00, -30:00:00.0, setup, 2026-04-22 12:24:00.000, 10
"""


def test_parse_vlbi_catalogue_preserves_headers_and_scan_order(tmp_path):
    path = tmp_path / "vlbi_cat_ep134f.csv"
    path.write_text(CATALOGUE_TEXT, encoding="utf-8")

    catalogue = parse_vlbi_catalogue(path)

    assert catalogue.obs_params["EXPERIMENT"] == "EP134F"
    assert catalogue.obs_params["POL"] == "XY"
    assert catalogue.obs_params["PCC_PREFIX"] == "mpcc"
    assert list(catalogue.obs_params["CHANNELS"]) == ["CH01", "CH02", "CH03", "CH04"]
    assert [scan.name for scan in catalogue.scans] == ["scan cal00", "scan No0001", "mpcc No0001", "setup"]
    assert catalogue.scans[1].target == "J1029A"
    assert catalogue.scans[1].duration == 60
    assert catalogue.scans[1].start_ts == datetime(2026, 4, 22, 12, 19, tzinfo=timezone.utc).timestamp()


def test_legacy_scan_dict_matches_antab_shape(tmp_path):
    path = tmp_path / "vlbi_cat_ep134f.csv"
    path.write_text(CATALOGUE_TEXT, encoding="utf-8")

    scan_data = legacy_scan_dict(parse_vlbi_catalogue(path))

    assert scan_data["scan No0001"]["target"] == "J1029A"
    assert scan_data["scan No0001"]["duration"] == 60
    assert scan_data["scan No0001"]["proc_duration"] == 62
    assert scan_data["scan No0001"]["proc_start_ts"] == pytest.approx(
        datetime(2026, 4, 22, 12, 18, 59, tzinfo=timezone.utc).timestamp()
    )


def test_scan_class_uses_catalogue_prefixes():
    obs_params = {"CAL_PREFIX": "scan cal", "PCC_PREFIX": "mpcc"}

    assert scan_class("scan cal00", obs_params) == "calibrator"
    assert scan_class("mpcc No0001", obs_params) == "pcc"
    assert scan_class("scan No0001", obs_params) == "science"
    assert scan_class("setup", obs_params) == "other"


def test_manifest_entries_from_catalogue_are_fail_closed(tmp_path):
    path = tmp_path / "vlbi_cat_ep134f.csv"
    path.write_text(CATALOGUE_TEXT, encoding="utf-8")
    catalogue = parse_vlbi_catalogue(path)

    entries = manifest_entries_from_catalogue(catalogue)

    assert [(entry.scan_id, entry.tags, entry.include, entry.reason) for entry in entries] == [
        ("scan cal00", ("calibrator",), False, "excluded calibrator scan"),
        ("scan No0001", ("science",), True, "science scan"),
        ("mpcc No0001", ("pcc",), False, "excluded phase-correction scan"),
        ("setup", ("other",), False, "excluded unclassified scan"),
    ]


def test_build_scan_manifest_serialises_stable_schema(tmp_path):
    path = tmp_path / "vlbi_cat_ep134f.csv"
    path.write_text(CATALOGUE_TEXT, encoding="utf-8")
    entries = manifest_entries_from_catalogue(parse_vlbi_catalogue(path))

    manifest = build_scan_manifest(
        entries,
        source="telstate_catalogue",
        generated_utc=datetime(2026, 6, 2, 12, 0, tzinfo=timezone.utc),
    )

    assert manifest["version"] == 1
    assert manifest["source"] == "telstate_catalogue"
    assert manifest["generated_utc"] == "2026-06-02T12:00:00Z"
    assert manifest["scans"][1] == {
        "scan_id": "scan No0001",
        "start_time": "2026-04-22T12:19:00Z",
        "end_time": "2026-04-22T12:20:00Z",
        "target_name": "J1029A",
        "tags": ["science"],
        "include": True,
        "reason": "science scan",
    }
