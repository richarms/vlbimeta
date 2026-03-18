"""Runtime helpers for controller-driven vlbimeta products."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class AntabProductPaths:
    """Filesystem layout for one ANTAB postprocessing product."""

    capture_root: Path
    vdif_dir: Path
    final_vdif_dir: Path
    writing_dir: Path
    final_dir: Path
    tsys_dir: Path
    metadata_path: Path

    @property
    def complete(self) -> bool:
        return self.final_dir.exists() and self.final_dir.is_dir() and self.metadata_path.exists()


def resolve_capture_root(data_dir: Path, capture_block_id: str) -> Path:
    capture_root = data_dir / capture_block_id
    if not capture_root.exists():
        raise FileNotFoundError(f"Capture root does not exist: {capture_root}")
    if not capture_root.is_dir():
        raise NotADirectoryError(f"Expected capture root directory: {capture_root}")
    return capture_root


def resolve_vdif_input_dir(capture_root: Path, capture_block_id: str) -> Path:
    preferred = capture_root / f"{capture_block_id}_vdif"
    fallback = capture_root / f"{capture_block_id}_vdif.writing"
    if preferred.exists():
        if not preferred.is_dir():
            raise NotADirectoryError(f"Expected completed VDIF directory: {preferred}")
        return preferred
    if fallback.exists():
        if not fallback.is_dir():
            raise NotADirectoryError(f"Expected in-progress VDIF directory: {fallback}")
        return fallback
    raise FileNotFoundError(
        f"Could not find either completed or in-progress VDIF product under {capture_root}"
    )


def antab_product_paths(data_dir: Path, capture_block_id: str) -> AntabProductPaths:
    capture_root = resolve_capture_root(data_dir, capture_block_id)
    writing_dir = capture_root / f"{capture_block_id}_antab.writing"
    final_dir = capture_root / f"{capture_block_id}_antab"
    return AntabProductPaths(
        capture_root=capture_root,
        vdif_dir=resolve_vdif_input_dir(capture_root, capture_block_id),
        final_vdif_dir=capture_root / f"{capture_block_id}_vdif",
        writing_dir=writing_dir,
        final_dir=final_dir,
        tsys_dir=writing_dir / "tsys",
        metadata_path=final_dir / "metadata.json",
    )


def prepare_writing_dir(paths: AntabProductPaths) -> None:
    paths.writing_dir.mkdir(parents=True, exist_ok=True)
    paths.tsys_dir.mkdir(parents=True, exist_ok=True)


def finalise_product_dir(paths: AntabProductPaths) -> None:
    if paths.final_dir.exists():
        return
    paths.writing_dir.rename(paths.final_dir)


def finalise_vdif_dir(paths: AntabProductPaths) -> Path:
    if paths.final_vdif_dir.exists():
        return paths.final_vdif_dir
    if paths.vdif_dir == paths.final_vdif_dir:
        return paths.final_vdif_dir
    paths.vdif_dir.rename(paths.final_vdif_dir)
    return paths.final_vdif_dir


def write_metadata_json(output_path: Path, payload: Mapping[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2, sort_keys=True)
        stream.write("\n")


def derive_experiment_name(obs_params: Mapping[str, Any] | None, override: str | None = None) -> str:
    if override:
        return override.strip()
    if not obs_params:
        raise KeyError("obs_params are not available to derive the VLBI experiment identifier")
    for key in ("proposal_id", "experiment", "experiment_id", "proposal-id"):
        value = obs_params.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    raise KeyError("obs_params do not contain a usable VLBI experiment identifier")


def resolve_catalogue_path(catalogue_dir: Path, experiment: str) -> Path:
    candidates = [
        catalogue_dir / f"vlbi_cat_{experiment}.csv",
        catalogue_dir / f"vlbi_cat_{experiment.lower()}.csv",
        catalogue_dir / f"vlbi_cat_{experiment.upper()}.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    joined = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"Catalogue not found for experiment '{experiment}'. Tried: {joined}")


def default_mean_power_sensor_keys(
    stream_name: str,
    channel_order: Sequence[str],
    sensor_pols: Sequence[str] = ("x", "y"),
) -> list[str]:
    if len(sensor_pols) != 2:
        raise ValueError(f"Expected exactly 2 sensor polarisation labels, got {len(sensor_pols)}")
    pol_map = {"pol0": sensor_pols[0], "pol1": sensor_pols[1]}
    sideband_map = {"lsb": 0, "usb": 1}
    sensor_keys: list[str] = []
    for channel_name in channel_order:
        sideband, pol_name = channel_name.split("-")
        try:
            sideband_index = sideband_map[sideband]
            sensor_pol = pol_map[pol_name]
        except KeyError as exc:
            raise ValueError(f"Unsupported channel mapping component in '{channel_name}'") from exc
        sensor_keys.append(f"{stream_name}.{sensor_pol}{sideband_index}.mean-power")
    return sensor_keys
