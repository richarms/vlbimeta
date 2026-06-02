import hashlib
from pathlib import Path

import pytest

from vlbimeta.runtime import (
    antab_product_paths,
    candidate_mean_power_sensor_key_sets,
    build_vdif_product_metadata,
    default_mean_power_sensor_keys,
    derive_experiment_name,
    finalise_product_dir,
    finalise_vdif_dir,
    iso_datetime_z,
    materialise_catalogue_from_telstate,
    prepare_writing_dir,
    resolve_catalogue_path,
    resolve_vdif_input_dir,
    shard_file_sizes,
    write_metadata_json,
)


def test_resolve_vdif_input_dir_prefers_completed(tmp_path: Path) -> None:
    capture_root = tmp_path
    writing = capture_root / "177_vdif.writing"
    final = capture_root / "177_vdif"
    writing.mkdir()
    final.mkdir()
    assert resolve_vdif_input_dir(capture_root, "177") == final


def test_antab_product_paths_and_finalise_top_level(tmp_path: Path) -> None:
    data_dir = tmp_path
    (data_dir / "177_vdif").mkdir()

    paths = antab_product_paths(data_dir, "177")
    prepare_writing_dir(paths)
    assert paths.writing_dir.exists()
    assert paths.tsys_dir.exists()
    assert paths.final_dir == data_dir / "177_antab"
    assert paths.final_vdif_dir == data_dir / "177_vdif"

    metadata = paths.writing_dir / "metadata.json"
    metadata.write_text("{}\n", encoding="utf-8")
    finalise_product_dir(paths)
    assert paths.final_dir.exists()
    assert (paths.final_dir / "metadata.json").exists()


def test_finalise_vdif_dir_renames_writing(tmp_path: Path) -> None:
    capture_root = tmp_path
    writing = capture_root / "177_vdif.writing"
    writing.mkdir()
    paths = antab_product_paths(tmp_path, "177")
    final_vdif_dir = finalise_vdif_dir(paths)
    assert final_vdif_dir == capture_root / "177_vdif"
    assert final_vdif_dir.exists()
    assert not writing.exists()


def test_finalise_vdif_dir_flattens_nested_scan_dir(tmp_path: Path) -> None:
    capture_root = tmp_path
    writing = capture_root / "177_vdif.writing"
    nested = writing / "177_vdif"
    nested.mkdir(parents=True)
    shard = nested / "177_vdif.00000000"
    shard.write_bytes(b"test")

    paths = antab_product_paths(tmp_path, "177")
    final_vdif_dir = finalise_vdif_dir(paths)

    assert final_vdif_dir == capture_root / "177_vdif"
    assert (final_vdif_dir / "177_vdif.00000000").read_bytes() == b"test"
    assert not nested.exists()


def test_antab_product_paths_supports_legacy_nested_layout(tmp_path: Path) -> None:
    legacy_capture_root = tmp_path / "177"
    legacy_capture_root.mkdir()
    (legacy_capture_root / "177_vdif").mkdir()

    paths = antab_product_paths(tmp_path, "177")

    assert paths.final_vdif_dir == legacy_capture_root / "177_vdif"
    assert paths.final_dir == legacy_capture_root / "177_antab"


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
    pytest.importorskip("pandas")
    pytest.importorskip("baseband")
    pytest.importorskip("matplotlib")
    pytest.importorskip("katdal")
    pytest.importorskip("katpoint")
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
