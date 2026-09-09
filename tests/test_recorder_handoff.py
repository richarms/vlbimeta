"""Cross-repository contract check; enable with KATSDPVLBI_SOURCE=/path/to/repo."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from vlbimeta.controller_entrypoint import main
from vlbimeta.runtime import product_paths, read_capture_manifest


@pytest.mark.skipif(not os.environ.get("KATSDPVLBI_SOURCE"), reason="Set KATSDPVLBI_SOURCE for the recorder integration check")
def test_recorder_to_postprocessor(tmp_path, monkeypatch):
    pytest.importorskip("aiokatcp")
    source = Path(os.environ["KATSDPVLBI_SOURCE"])
    spec = importlib.util.spec_from_file_location("handoff_recorder", source / "scripts/jive5ab_katcp_proxy.py")
    recorder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(recorder)
    assert (source / "docs/handoff.md").read_text() == (
        Path(__file__).parents[1] / "docs/handoff.md"
    ).read_text()
    monkeypatch.setenv("DISK_PATHS", str(tmp_path))

    async def capture(stream):
        monkeypatch.setenv("VLBI_STREAM_NAME", stream)
        server = recorder.Jive5abServer("127.0.0.1", 0, 2620)
        monkeypatch.setattr(server, "_poll_once", AsyncMock())
        monkeypatch.setattr(recorder, "jive_cmd", AsyncMock(return_value="!record= 0 ;"))
        await server.request_capture_init(None, "177")
        paths = product_paths(tmp_path, "177", stream)
        with pytest.raises(ValueError, match="unfinished"):
            main([str(tmp_path), "177", stream, "--mode", "pass_through"])
        # Model jive5ab's nested VBS directory, without claiming valid VDIF data.
        nested = paths.capture_root / "raw.writing" / f"177_{stream}"
        nested.mkdir()
        (nested / f"177_{stream}.00000000").write_bytes(b"recorded payload")
        await server.request_capture_done(None)
        manifest = read_capture_manifest(paths)
        assert main([str(tmp_path), "177", stream, "--mode", "pass_through"]) == 0
        assert paths.complete
        assert (paths.final_vdif_dir / f"177_{stream}.00000000").read_bytes() == b"recorded payload"
        assert read_capture_manifest(paths) == manifest
        metadata = json.loads((paths.final_vdif_dir / "metadata.json").read_text())
        assert metadata["StreamId"] == stream
        assert metadata["FileSize"] == [16]

    asyncio.run(capture("sdp_vdif"))
    asyncio.run(capture("sdp_vdif_metadata"))
