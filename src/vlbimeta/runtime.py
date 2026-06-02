"""Runtime helpers for controller-driven vlbimeta products."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

VLBI_EXPERIMENT_KEY = "vlbi_experiment"
VLBI_CATALOGUE_CONTENT_KEY = "vlbi_catalogue_content"
VLBI_CATALOGUE_SHA256_KEY = "vlbi_catalogue_sha256"
VLBI_CATALOGUE_NAME_KEY = "vlbi_catalogue_name"
VLBI_CATALOGUE_FORMAT_KEY = "vlbi_catalogue_format"


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
    legacy_capture_root = data_dir / capture_block_id
    if legacy_capture_root.exists():
        if not legacy_capture_root.is_dir():
            raise NotADirectoryError(f"Expected capture root directory: {legacy_capture_root}")
        return legacy_capture_root
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory does not exist: {data_dir}")
    if not data_dir.is_dir():
        raise NotADirectoryError(f"Expected data directory: {data_dir}")
    return data_dir


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
    raise FileNotFoundError(f"Could not find either completed or in-progress VDIF product under {capture_root}")


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


def _collapse_nested_vdif_dir(source_dir: Path, final_basename: str) -> None:
    nested_dir = source_dir / final_basename
    if not nested_dir.is_dir():
        return
    for child in nested_dir.iterdir():
        destination = source_dir / child.name
        if destination.exists():
            raise FileExistsError(f"Refusing to overwrite existing path while flattening VDIF output: {destination}")
        child.rename(destination)
    nested_dir.rmdir()


def finalise_vdif_dir(paths: AntabProductPaths) -> Path:
    if paths.final_vdif_dir.exists():
        return paths.final_vdif_dir
    if paths.vdif_dir == paths.final_vdif_dir:
        return paths.final_vdif_dir
    _collapse_nested_vdif_dir(paths.vdif_dir, paths.final_vdif_dir.name)
    paths.vdif_dir.rename(paths.final_vdif_dir)
    return paths.final_vdif_dir


def write_metadata_json(output_path: Path, payload: Mapping[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2, sort_keys=True)
        stream.write("\n")


def iso_datetime_z(value: Any, fallback: datetime | None = None) -> str:
    """Normalise supported time inputs to a Solr-compatible UTC timestamp."""
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith("Z"):
            return text
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
        value = parsed
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    fallback_value = fallback or datetime.now(timezone.utc)
    return fallback_value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def shard_file_sizes(final_vdif_dir: Path) -> list[int]:
    """Return file sizes for shard files in stable lexical order."""
    shard_paths = sorted(
        path
        for path in final_vdif_dir.iterdir()
        if path.is_file() and path.name != "metadata.json"
    )
    return [path.stat().st_size for path in shard_paths]


def _obs_list(obs_params: Mapping[str, Any] | None, key: str) -> list[str]:
    if not obs_params:
        return []
    raw_value = obs_params.get(key)
    if isinstance(raw_value, str) and raw_value.strip():
        return [raw_value.strip()]
    if not isinstance(raw_value, Sequence) or isinstance(raw_value, (bytes, bytearray)):
        return []
    values: list[str] = []
    for item in raw_value:
        if isinstance(item, str) and item.strip():
            values.append(item.strip())
    return values


def build_vdif_product_metadata(
    *,
    capture_block_id: str,
    stream_name: str,
    obs_params: Mapping[str, Any] | None,
    final_vdif_dir: Path,
    run: int = 1,
    fallback_start: datetime | None = None,
) -> dict[str, Any]:
    """Build first-pass DLM ingest metadata for a completed VDIF product."""
    description = "MeerKAT VLBI VDIF recording"
    schedule_block_id = "19700101-0000"
    proposal_id = "UNKNOWN"
    observer = "unknown"
    if obs_params:
        description = str(obs_params.get("description") or description)
        schedule_block_id = str(obs_params.get("sb_id_code") or schedule_block_id)
        proposal_id = str(obs_params.get("proposal_id") or proposal_id)
        observer = str(obs_params.get("observer") or observer)
    metadata: dict[str, Any] = {
        "CaptureBlockId": str(capture_block_id),
        "Description": description,
        "FileSize": shard_file_sizes(final_vdif_dir),
        "Observer": observer,
        "ProductType": {
            "ProductTypeName": "VDIFProduct",
            "ReductionName": "VDIF Data",
        },
        "ProposalId": proposal_id,
        "Run": int(run),
        "ScheduleBlockIdCode": schedule_block_id,
        "StartTime": iso_datetime_z(
            obs_params.get("start_time") if obs_params else None,
            fallback=fallback_start,
        ),
        "StreamId": stream_name,
    }
    targets = _obs_list(obs_params, "targets")
    if targets:
        metadata["Targets"] = targets
        metadata["KatpointTargets"] = targets
    if obs_params:
        bandwidth = obs_params.get("bandwidth")
        if isinstance(bandwidth, (int, float)):
            metadata["Bandwidth"] = float(bandwidth)
        centre_frequency = obs_params.get("centre_frequency")
        if isinstance(centre_frequency, (int, float)):
            metadata["CenterFrequency"] = float(centre_frequency)
        antennas = _obs_list(obs_params, "antennas")
        if antennas:
            metadata["Antennas"] = antennas
    return metadata


def derive_experiment_name(obs_params: Mapping[str, Any] | None, override: str | None = None) -> str:
    if override:
        return override.strip()
    if not obs_params:
        raise KeyError("obs_params are not available to derive the VLBI experiment identifier")
    vlbi = obs_params.get("vlbi")
    if isinstance(vlbi, Mapping):
        value = vlbi.get("experiment")
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
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


def materialise_catalogue_from_telstate(
    telstate: Any,
    output_dir: Path,
    fallback_experiment: str | None = None,
) -> tuple[Path | None, dict[str, str]]:
    if VLBI_CATALOGUE_CONTENT_KEY not in telstate:
        return None, {}
    content = telstate[VLBI_CATALOGUE_CONTENT_KEY]
    if isinstance(content, bytes):
        content_bytes = content
        content_text = content.decode("utf-8")
    elif isinstance(content, str):
        content_text = content
        content_bytes = content.encode("utf-8")
    else:
        raise TypeError(f"Expected telstate catalogue content to be str or bytes, got {type(content)!r}")
    actual_sha256 = hashlib.sha256(content_bytes).hexdigest()
    expected_sha256 = telstate[VLBI_CATALOGUE_SHA256_KEY] if VLBI_CATALOGUE_SHA256_KEY in telstate else None
    if expected_sha256 is not None and expected_sha256 != actual_sha256:
        raise ValueError(
            f"VLBI catalogue checksum mismatch: telstate has {expected_sha256}, materialised content is {actual_sha256}"
        )
    raw_name = telstate[VLBI_CATALOGUE_NAME_KEY] if VLBI_CATALOGUE_NAME_KEY in telstate else None
    if isinstance(raw_name, str) and raw_name.strip():
        catalogue_name = Path(raw_name).name
    elif fallback_experiment:
        catalogue_name = f"vlbi_cat_{fallback_experiment}.csv"
    else:
        catalogue_name = "vlbi_catalogue.csv"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / catalogue_name
    output_path.write_text(content_text, encoding="utf-8")
    info = {
        "catalogue_file": catalogue_name,
        "catalogue_sha256": actual_sha256,
        "catalogue_source": "telstate",
    }
    if VLBI_CATALOGUE_FORMAT_KEY in telstate:
        catalogue_format = telstate[VLBI_CATALOGUE_FORMAT_KEY]
        if isinstance(catalogue_format, str) and catalogue_format.strip():
            info["catalogue_format"] = catalogue_format.strip()
    return output_path, info


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


def candidate_mean_power_sensor_key_sets(
    stream_name: str,
    channel_order: Sequence[str],
    sensor_pols: Sequence[str] = ("x", "y"),
) -> list[list[str]]:
    prefixes = [stream_name]
    if stream_name == "sdp_vdif":
        prefixes.extend(["gpucbf_tied_array_resampled_voltage", "tied_array_resampled_voltage"])
    seen: set[str] = set()
    key_sets: list[list[str]] = []
    for prefix in prefixes:
        if prefix in seen:
            continue
        seen.add(prefix)
        key_sets.append(default_mean_power_sensor_keys(prefix, channel_order, sensor_pols=sensor_pols))
    return key_sets
