"""Preparation preserves fresh CAM allocation and explicitly selects pass-through."""

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path

import pytest

from vlbimeta.catalogue import parse_vlbi_catalogue_content


SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
spec = importlib.util.spec_from_file_location("prepare_cam_smoke", SCRIPTS / "prepare_cam_smoke.py")
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


def config():
    return dict(version="4.9", config={"other": 3, "image_overrides": {"cal": "keep"}}, inputs={
        "x": {"type": "cbf.tied_array_channelised_voltage"},
        "y": {"type": "cbf.tied_array_channelised_voltage"},
        "v": {"type": "cbf.tied_array_resampled_voltage", "src_streams": ["x", "y"],
              "url": "vdif://239.192.2.9:7148"},
    }, outputs={"sdp_vdif": {"type": "sdp.vdif", "src_streams": ["v"]}})


def test_overlay_preserves_allocation():
    original = config()
    images = json.loads((SCRIPTS / "cam_smoke_images.json").read_text())
    result = smoke.prepare(original, images, "sdp_vdif")
    assert result["inputs"] == original["inputs"]
    assert result["outputs"] == original["outputs"]
    assert result["config"]["other"] == 3
    assert result["config"]["image_overrides"]["cal"] == "keep"
    assert result["config"]["vlbimeta"]["mode"] == "pass_through"
    assert "vlbimeta" not in original["config"]


def test_wrong_stream_protocol_rejected():
    original = config()
    original["inputs"]["v"]["url"] = "spead://239.192.2.9:7148"
    images = json.loads((SCRIPTS / "cam_smoke_images.json").read_text())
    with pytest.raises(ValueError, match="vdif://"):
        smoke.prepare(original, images, "sdp_vdif")


def test_mutable_image_tag_rejected():
    images = json.loads((SCRIPTS / "cam_smoke_images.json").read_text())
    images["vlbimeta"] = "vlbimeta:latest"
    with pytest.raises(ValueError, match="digest-pinned"):
        smoke.prepare(config(), images, "sdp_vdif")


def test_csv_has_observer_headers_and_tracking_windows():
    start = datetime(2026, 9, 24, 12, tzinfo=timezone.utc)
    catalogue = parse_vlbi_catalogue_content(smoke.catalogue(start).encode())
    assert catalogue.obs_params["PCC_VERIFICATION"] == "False"
    assert [scan.name for scan in catalogue.scans] == ["scan No0001", "scan No0002"]
    assert [scan.start_ts - start.timestamp() for scan in catalogue.scans] == [0, 40]
    assert all(scan.duration == 20 for scan in catalogue.scans)
    with pytest.raises(ValueError, match="explicit UTC"):
        smoke.catalogue(start.replace(tzinfo=None))
