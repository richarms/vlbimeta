import hashlib
import json
from pathlib import Path

import pytest

from vlbimeta.runtime import (
    product_paths,
    read_capture_manifest,
    candidate_mean_power_sensor_key_sets,
    build_vdif_product_metadata,
    default_mean_power_sensor_keys,
    derive_experiment_name,
    finalise_products,
    stage_vdif_product,
    iso_datetime_z,
    materialise_catalogue_from_telstate,
    prepare_writing_dir,
    resolve_catalogue_path,
    shard_file_sizes,
    write_metadata_json,
)


def make_capture(data_dir: Path, cbid: str = "177", stream: str = "sdp_vdif"):
    paths = product_paths(data_dir, cbid, stream)
    paths.vdif_dir.mkdir(parents=True)
    name = f"{cbid}_{stream}.00000000"
    (paths.vdif_dir / name).write_bytes(b"vdif")
    manifest = {
        "version": 1,
        "capture_block_id": cbid,
        "stream_name": stream,
        "status": "closed",
        "shards": [{"name": name, "size_bytes": 4}],
    }
    write_metadata_json(paths.vdif_dir / "capture.json", manifest)
    return paths, manifest


def test_product_paths_are_stream_specific(tmp_path: Path) -> None:
    paths = product_paths(tmp_path, "177", "sdp_vdif")
    assert paths.vdif_dir == tmp_path / ".vlbi/177/sdp_vdif/raw"
    assert paths.vdif_writing_dir == tmp_path / "177_sdp_vdif.vdif.writing"
    assert paths.final_vdif_dir == tmp_path / "177_sdp_vdif.vdif"
    assert paths.writing_dir == tmp_path / "177_sdp_vdif.metadata.writing"
    assert paths.final_dir == tmp_path / "177_sdp_vdif.metadata"
    other = product_paths(tmp_path, "177", "other")
    assert other.vdif_dir != paths.vdif_dir
    assert other.final_dir != paths.final_dir
    assert other.final_vdif_dir != paths.final_vdif_dir


@pytest.mark.parametrize("value", ["", "../177", "a/b", r"a\b", ".", "..", "a..b", "a b", "177\n"])
def test_product_paths_reject_invalid_identifiers(tmp_path: Path, value: str) -> None:
    with pytest.raises(ValueError):
        product_paths(tmp_path, value, "sdp_vdif")
    with pytest.raises(ValueError):
        product_paths(tmp_path, "177", value)


@pytest.mark.parametrize("legacy", ["177_vdif", "177_vdif.writing", "177/177_vdif", "177/177_vdif.writing"])
def test_legacy_capture_is_not_automatically_adopted(tmp_path: Path, legacy: str) -> None:
    (tmp_path / legacy).mkdir(parents=True)
    with pytest.raises(FileNotFoundError, match="Closed raw capture required"):
        read_capture_manifest(product_paths(tmp_path, "177", "sdp_vdif"))


def test_writing_capture_is_rejected_even_with_manifest(tmp_path: Path) -> None:
    paths, _ = make_capture(tmp_path)
    paths.vdif_dir.rename(paths.capture_root / "raw.writing")
    with pytest.raises(ValueError, match="unfinished"):
        read_capture_manifest(paths)


@pytest.mark.parametrize("change", ["version", "identity", "stream", "status", "empty", "duplicate", "truncated", "extra", "missing", "symlink", "traversal"])
def test_invalid_capture_is_rejected(tmp_path: Path, change: str) -> None:
    paths, manifest = make_capture(tmp_path)
    shard = paths.vdif_dir / manifest["shards"][0]["name"]
    if change == "version":
        manifest["version"] = 2
    elif change == "identity":
        manifest["capture_block_id"] = "178"
    elif change == "stream":
        manifest["stream_name"] = "other"
    elif change == "status":
        manifest["status"] = "writing"
    elif change == "empty":
        manifest["shards"] = []
    elif change == "duplicate":
        manifest["shards"] *= 2
    elif change == "truncated":
        shard.write_bytes(b"v")
    elif change == "extra":
        (paths.vdif_dir / "unexpected").write_bytes(b"x")
    elif change == "missing":
        shard.unlink()
    elif change == "symlink":
        shard.unlink()
        target = tmp_path / "external"
        target.write_bytes(b"vdif")
        shard.symlink_to(target)
    elif change == "traversal":
        manifest["shards"][0]["name"] = "../outside"
    write_metadata_json(paths.vdif_dir / "capture.json", manifest)
    with pytest.raises(ValueError):
        read_capture_manifest(paths)


def test_publish_preserves_raw_and_publishes_metadata_last(tmp_path: Path) -> None:
    paths, manifest = make_capture(tmp_path)
    original = {p.name: p.read_bytes() for p in paths.vdif_dir.iterdir()}
    assert read_capture_manifest(paths) == manifest
    prepare_writing_dir(paths)
    staged = stage_vdif_product(paths)
    name = manifest["shards"][0]["name"]
    assert (staged / name).stat().st_ino == (paths.vdif_dir / name).stat().st_ino
    assert not paths.final_dir.exists()
    assert not paths.final_vdif_dir.exists()
    with pytest.raises(FileNotFoundError, match="without metadata"):
        finalise_products(paths)
    write_metadata_json(staged / "metadata.json", {"StreamId": "sdp_vdif"})
    write_metadata_json(paths.writing_dir / "metadata.json", {"status": "pass_through"})
    finalise_products(paths)
    assert paths.complete
    assert (paths.final_vdif_dir / name).read_bytes() == b"vdif"
    assert (paths.final_vdif_dir / "metadata.json").exists()
    assert not paths.writing_dir.exists()
    assert not paths.vdif_writing_dir.exists()
    assert {p.name: p.read_bytes() for p in paths.vdif_dir.iterdir()} == original


@pytest.mark.parametrize("existing", ["final_dir", "final_vdif_dir", "writing_dir", "vdif_writing_dir"])
def test_existing_output_requires_explicit_recovery(tmp_path: Path, existing: str) -> None:
    paths, _ = make_capture(tmp_path)
    getattr(paths, existing).mkdir()
    with pytest.raises(FileExistsError, match="explicit recovery"):
        prepare_writing_dir(paths)


def test_pass_through_entrypoint(tmp_path: Path) -> None:
    from vlbimeta.controller_entrypoint import main

    paths, manifest = make_capture(tmp_path)
    assert main([str(tmp_path), "177", "sdp_vdif", "--mode", "pass_through"]) == 0
    metadata = json.loads(paths.metadata_path.read_text())
    assert metadata["input_vdif_dir"] == str(paths.vdif_dir)
    assert metadata["final_vdif_dir"] == str(paths.final_vdif_dir)
    assert metadata["handoff_version"] == 1
    assert metadata["vdif_selection"] == "full_capture"
    assert json.loads((paths.final_dir / "capture.json").read_text()) == manifest
    vdif_metadata = json.loads((paths.final_vdif_dir / "metadata.json").read_text())
    assert vdif_metadata["FileSize"] == [4]
    assert vdif_metadata["StreamId"] == "sdp_vdif"
    assert paths.vdif_dir.is_dir()
    with pytest.raises(FileExistsError, match="explicit recovery"):
        main([str(tmp_path), "177", "sdp_vdif", "--mode", "pass_through"])
    with pytest.raises(FileExistsError, match="explicit recovery"):
        main([str(tmp_path), "177", "sdp_vdif", "--mode", "antab"])


def test_disabled_does_not_require_capture_or_create_output(tmp_path: Path) -> None:
    from vlbimeta.controller_entrypoint import main

    assert main([str(tmp_path / "missing"), "177", "sdp_vdif", "--mode", "disabled"]) == 0
    assert list(tmp_path.iterdir()) == []


def test_entrypoint_failure_keeps_raw_unpublished(tmp_path: Path, monkeypatch) -> None:
    from vlbimeta import controller_entrypoint

    paths, manifest = make_capture(tmp_path)
    def fail(**kwargs):
        raise RuntimeError("metadata failed")
    monkeypatch.setattr(controller_entrypoint, "build_vdif_product_metadata", fail)
    with pytest.raises(RuntimeError, match="metadata failed"):
        controller_entrypoint.main([str(tmp_path), "177", "sdp_vdif", "--mode", "pass_through"])
    assert read_capture_manifest(paths) == manifest
    assert not paths.final_vdif_dir.exists()
    assert not paths.final_dir.exists()
    assert paths.vdif_writing_dir.is_dir()
    assert paths.writing_dir.is_dir()


def test_shard_file_sizes_ignores_metadata(tmp_path: Path) -> None:
    final_vdif_dir = tmp_path / "177_vdif"
    final_vdif_dir.mkdir()
    (final_vdif_dir / "177_vdif.00000000").write_bytes(b"a" * 3)
    (final_vdif_dir / "177_vdif.00000001").write_bytes(b"b" * 5)
    (final_vdif_dir / "metadata.json").write_text("{}\n", encoding="utf-8")

    assert shard_file_sizes(final_vdif_dir) == [3, 5]


def test_iso_datetime_z_normalises_iso_strings() -> None:
    assert iso_datetime_z("2026-03-31T09:47:47.202591+00:00") == "2026-03-31T09:47:47Z"


def test_build_vdif_product_metadata(tmp_path: Path) -> None:
    obs_params = {
        "description": "vdif ingest verification",
        "observer": "testy",
        "proposal_id": "ES116A",
        "sb_id_code": "20260331-0001",
        "start_time": "2026-03-31T09:47:47.202591+00:00",
        "targets": ["Zenith, azel, 0, 90"],
    }
    final_vdif_dir = tmp_path / "177_vdif"
    final_vdif_dir.mkdir()

    metadata = build_vdif_product_metadata(
        capture_block_id="1774950408",
        stream_name="sdp_vdif",
        obs_params=obs_params,
        final_vdif_dir=final_vdif_dir,
        run=1,
    )

    assert metadata == {
        "CaptureBlockId": "1774950408",
        "Description": "vdif ingest verification",
        "FileSize": [],
        "KatpointTargets": ["Zenith, azel, 0, 90"],
        "Observer": "testy",
        "ProductType": {
            "ProductTypeName": "VDIFProduct",
            "ReductionName": "VDIF Data",
        },
        "ProposalId": "ES116A",
        "Run": 1,
        "ScheduleBlockIdCode": "20260331-0001",
        "StartTime": "2026-03-31T09:47:47Z",
        "StreamId": "sdp_vdif",
        "Targets": ["Zenith, azel, 0, 90"],
    }


def test_vdif_metadata_json_written_into_final_vdif_dir(tmp_path: Path) -> None:
    final_vdif_dir = tmp_path / "177_vdif"
    final_vdif_dir.mkdir()
    payload = {"CaptureBlockId": "177", "ProductType": {"ProductTypeName": "VDIFProduct", "ReductionName": "VDIF Data"}}

    write_metadata_json(final_vdif_dir / "metadata.json", payload)

    assert (final_vdif_dir / "metadata.json").exists()


def test_derive_experiment_name_prefers_override() -> None:
    obs_params = {"proposal_id": "ignored"}
    assert derive_experiment_name(obs_params, "custom") == "custom"


def test_derive_experiment_name_prefers_vlbi_metadata() -> None:
    obs_params = {"proposal_id": "ignored", "vlbi": {"experiment": "EP134F"}}
    assert derive_experiment_name(obs_params) == "ep134f"


def test_derive_experiment_name_uses_proposal_id() -> None:
    obs_params = {"proposal_id": "ES116A"}
    assert derive_experiment_name(obs_params) == "es116a"


def test_resolve_catalogue_path_normalises_case(tmp_path: Path) -> None:
    catalogue = tmp_path / "vlbi_cat_es116a.csv"
    catalogue.write_text("# EXPERIMENT ES116A\n", encoding="utf-8")
    assert resolve_catalogue_path(tmp_path, "ES116A") == catalogue


def test_materialise_catalogue_from_telstate_writes_verified_file(tmp_path: Path) -> None:
    content = "# EXPERIMENT EP134F\n"
    checksum = hashlib.sha256(content.encode("utf-8")).hexdigest()
    telstate = {
        "vlbi_catalogue_content": content,
        "vlbi_catalogue_name": "vlbi_cat_ep134f.csv",
        "vlbi_catalogue_sha256": checksum,
        "vlbi_catalogue_format": "csv",
    }

    path, info = materialise_catalogue_from_telstate(telstate, tmp_path, fallback_experiment="ep134f")

    assert path == tmp_path / "vlbi_cat_ep134f.csv"
    assert path.read_text(encoding="utf-8") == content
    assert info == {
        "catalogue_file": "vlbi_cat_ep134f.csv",
        "catalogue_format": "csv",
        "catalogue_sha256": checksum,
        "catalogue_source": "telstate",
    }


def test_materialise_catalogue_from_telstate_rejects_bad_checksum(tmp_path: Path) -> None:
    telstate = {
        "vlbi_catalogue_content": "# EXPERIMENT EP134F\n",
        "vlbi_catalogue_sha256": "deadbeef",
    }

    with pytest.raises(ValueError, match="checksum mismatch"):
        materialise_catalogue_from_telstate(telstate, tmp_path, fallback_experiment="ep134f")


def test_controller_parse_args_dataset_stream_name() -> None:
    from vlbimeta.controller_entrypoint import parse_args

    args = parse_args(["/tmp/data", "177", "sdp_vdif", "--dataset-stream-name", "sdp_l0"])
    assert args.dataset_stream_name == "sdp_l0"


def test_default_mean_power_sensor_keys() -> None:
    keys = default_mean_power_sensor_keys(
        "sdp_vdif",
        ["lsb-pol0", "lsb-pol1", "usb-pol0", "usb-pol1"],
    )
    assert keys == [
        "sdp_vdif.x0.mean-power",
        "sdp_vdif.y0.mean-power",
        "sdp_vdif.x1.mean-power",
        "sdp_vdif.y1.mean-power",
    ]


def test_default_mean_power_sensor_keys_requires_two_pols() -> None:
    with pytest.raises(ValueError):
        default_mean_power_sensor_keys("sdp_vdif", ["lsb-pol0"], sensor_pols=("x",))


def test_candidate_mean_power_sensor_key_sets_for_vdif() -> None:
    key_sets = candidate_mean_power_sensor_key_sets(
        "sdp_vdif",
        ["lsb-pol0", "lsb-pol1", "usb-pol0", "usb-pol1"],
    )
    assert key_sets == [
        [
            "sdp_vdif.x0.mean-power",
            "sdp_vdif.y0.mean-power",
            "sdp_vdif.x1.mean-power",
            "sdp_vdif.y1.mean-power",
        ],
        [
            "gpucbf_tied_array_resampled_voltage.x0.mean-power",
            "gpucbf_tied_array_resampled_voltage.y0.mean-power",
            "gpucbf_tied_array_resampled_voltage.x1.mean-power",
            "gpucbf_tied_array_resampled_voltage.y1.mean-power",
        ],
        [
            "tied_array_resampled_voltage.x0.mean-power",
            "tied_array_resampled_voltage.y0.mean-power",
            "tied_array_resampled_voltage.x1.mean-power",
            "tied_array_resampled_voltage.y1.mean-power",
        ],
    ]


def test_capture_id_cannot_collide_with_stream_separator(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="decimal digits"):
        product_paths(tmp_path, "177_other", "sdp_vdif")


@pytest.mark.parametrize("fail_generation", [False, True])
def test_antab_uses_same_handoff_and_publication_paths(tmp_path: Path, monkeypatch, fail_generation) -> None:
    from vlbimeta import controller_entrypoint, telstate_antab_from_mean_power

    paths, manifest = make_capture(tmp_path)
    catalogue = Path(__file__).parents[1] / "src/vlbimeta/catalogues/vlbi_cat_es116a.csv"
    obs_params = {"proposal_id": "ES116A"}
    telstate = {"vlbi_catalogue_content": catalogue.read_text()}
    monkeypatch.setattr(controller_entrypoint, "_derive_obs_params", lambda *args: obs_params)
    monkeypatch.setattr(controller_entrypoint, "_open_capture_telstate", lambda *args: telstate)

    def generate(**kwargs):
        assert kwargs["metadata_dir"] == paths.writing_dir
        assert kwargs["power_dir"] == paths.tsys_dir
        if fail_generation:
            raise RuntimeError("ANTAB generation failed")
        output = kwargs["metadata_dir"] / "es116ame.antab"
        output.write_text("simulated ANTAB output\n")
        return output

    monkeypatch.setattr(telstate_antab_from_mean_power, "generate_antab_from_capture", generate)
    args = [str(tmp_path), "177", "sdp_vdif", "--mode", "antab", "--telstate", "unused:6379"]
    if fail_generation:
        with pytest.raises(RuntimeError, match="ANTAB generation failed"):
            controller_entrypoint.main(args)
        assert not paths.final_vdif_dir.exists()
        assert not paths.final_dir.exists()
    else:
        assert controller_entrypoint.main(args) == 0
        assert (paths.final_dir / "es116ame.antab").is_file()
        assert (paths.final_dir / "scan_manifest.json").is_file()
        assert (paths.final_vdif_dir / "metadata.json").is_file()
        metadata = json.loads(paths.metadata_path.read_text())
        assert metadata["final_vdif_dir"] == str(paths.final_vdif_dir)
        assert metadata["vdif_selection"] == "full_capture"
    assert read_capture_manifest(paths) == manifest
