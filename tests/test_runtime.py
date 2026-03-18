from pathlib import Path

import pytest

from vlbimeta.runtime import (
    antab_product_paths,
    default_mean_power_sensor_keys,
    derive_experiment_name,
    finalise_product_dir,
    finalise_vdif_dir,
    prepare_writing_dir,
    resolve_catalogue_path,
    resolve_vdif_input_dir,
)


def test_resolve_vdif_input_dir_prefers_completed(tmp_path: Path) -> None:
    capture_root = tmp_path / "177"
    capture_root.mkdir()
    writing = capture_root / "177_vdif.writing"
    final = capture_root / "177_vdif"
    writing.mkdir()
    final.mkdir()
    assert resolve_vdif_input_dir(capture_root, "177") == final


def test_antab_product_paths_and_finalise(tmp_path: Path) -> None:
    data_dir = tmp_path
    capture_root = data_dir / "177"
    capture_root.mkdir()
    (capture_root / "177_vdif").mkdir()

    paths = antab_product_paths(data_dir, "177")
    prepare_writing_dir(paths)
    assert paths.writing_dir.exists()
    assert paths.tsys_dir.exists()

    metadata = paths.writing_dir / "metadata.json"
    metadata.write_text("{}\n", encoding="utf-8")
    finalise_product_dir(paths)
    assert paths.final_dir.exists()
    assert (paths.final_dir / "metadata.json").exists()


def test_finalise_vdif_dir_renames_writing(tmp_path: Path) -> None:
    capture_root = tmp_path / "177"
    capture_root.mkdir()
    writing = capture_root / "177_vdif.writing"
    writing.mkdir()
    paths = antab_product_paths(tmp_path, "177")
    final_vdif_dir = finalise_vdif_dir(paths)
    assert final_vdif_dir == capture_root / "177_vdif"
    assert final_vdif_dir.exists()
    assert not writing.exists()


def test_derive_experiment_name_prefers_override() -> None:
    obs_params = {"proposal_id": "ignored"}
    assert derive_experiment_name(obs_params, "custom") == "custom"


def test_derive_experiment_name_uses_proposal_id() -> None:
    obs_params = {"proposal_id": "ES116A"}
    assert derive_experiment_name(obs_params) == "es116a"


def test_resolve_catalogue_path_normalises_case(tmp_path: Path) -> None:
    catalogue = tmp_path / "vlbi_cat_es116a.csv"
    catalogue.write_text("# EXPERIMENT ES116A\n", encoding="utf-8")
    assert resolve_catalogue_path(tmp_path, "ES116A") == catalogue


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
