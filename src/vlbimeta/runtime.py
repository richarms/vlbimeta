"""Runtime helpers for controller-driven vlbimeta products."""

from __future__ import annotations

import hashlib
import json
import os
import re
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
class VlbiProductPaths:
    """Version 1 recorder handoff and stream-specific published products."""

    capture_root: Path
    vdif_dir: Path
    vdif_writing_dir: Path
    final_vdif_dir: Path
    writing_dir: Path
    final_dir: Path
    tsys_dir: Path
    metadata_path: Path

    @property
    def complete(self) -> bool:
        return self.final_dir.exists() and self.final_dir.is_dir() and self.metadata_path.exists()


def validate_path_component(value: str) -> str:
    """Use the same identifier grammar as the recorder handoff contract."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value) or ".." in value:
        raise ValueError(f"Invalid capture/stream identifier: {value!r}")
    return value


def product_paths(data_dir: Path, capture_block_id: str, stream_name: str) -> VlbiProductPaths:
    if not re.fullmatch(r"[0-9]+", capture_block_id):
        raise ValueError(f"Invalid capture block ID (decimal digits required): {capture_block_id!r}")
    cbid = capture_block_id
    stream = validate_path_component(stream_name)
    capture_root = data_dir / ".vlbi" / cbid / stream
    basename = f"{cbid}_{stream}"
    writing_dir = data_dir / f"{basename}.metadata.writing"
    final_dir = data_dir / f"{basename}.metadata"
    return VlbiProductPaths(
        capture_root=capture_root,
        vdif_dir=capture_root / "raw",
        vdif_writing_dir=data_dir / f"{basename}.vdif.writing",
        final_vdif_dir=data_dir / f"{basename}.vdif",
        writing_dir=writing_dir,
        final_dir=final_dir,
        tsys_dir=writing_dir / "tsys",
        metadata_path=final_dir / "metadata.json",
    )


def read_capture_manifest(paths: VlbiProductPaths) -> dict[str, Any]:
    """Require a recorder-closed capture with an exact, non-empty shard inventory.

    No legacy path discovery or recovery of raw.writing is performed here. Sizes
    detect truncated/changed files; this is not a VDIF payload integrity check.
    """
    if (paths.capture_root / "raw.writing").exists():
        raise ValueError(f"Capture is unfinished or ambiguous: {paths.capture_root}")
    if paths.vdif_dir.is_symlink() or not paths.vdif_dir.is_dir():
        raise FileNotFoundError(f"Closed raw capture required: {paths.vdif_dir}")
    manifest_path = paths.vdif_dir / "capture.json"
    if manifest_path.is_symlink():
        raise ValueError(f"Capture manifest must be a regular file: {manifest_path}")
    with manifest_path.open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    cbid = paths.capture_root.parent.name
    stream_name = paths.capture_root.name
    expected = {"version": 1, "capture_block_id": cbid, "stream_name": stream_name, "status": "closed"}
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("version")) is not int
        or any(manifest.get(key) != value for key, value in expected.items())
    ):
        raise ValueError(f"Invalid capture identity, status or handoff version: {manifest_path}")
    shards = manifest.get("shards")
    if not isinstance(shards, list) or not shards:
        raise ValueError("Closed capture must contain at least one shard")
    names: set[str] = set()
    for shard in shards:
        if not isinstance(shard, dict):
            raise ValueError("Invalid capture shard entry")
        name, size = shard.get("name"), shard.get("size_bytes")
        if not isinstance(name, str) or not re.fullmatch(re.escape(f"{cbid}_{stream_name}") + r"\.[0-9]+", name):
            raise ValueError(f"Invalid capture shard name: {name!r}")
        if name in names or type(size) is not int or size <= 0:
            raise ValueError(f"Duplicate or empty capture shard: {name}")
        names.add(name)
        path = paths.vdif_dir / name
        if path.is_symlink() or not path.is_file() or path.stat().st_size != size:
            raise ValueError(f"Capture shard missing or changed: {path}")
    if {path.name for path in paths.vdif_dir.iterdir()} != names | {"capture.json"}:
        raise ValueError(f"Capture inventory does not match directory: {paths.vdif_dir}")
    return manifest


def prepare_writing_dir(paths: VlbiProductPaths) -> None:
    """Reserve fresh output directories; never interpret an old output as success."""
    for path in (paths.final_dir, paths.final_vdif_dir, paths.writing_dir, paths.vdif_writing_dir):
        if path.exists() or path.is_symlink():
            raise FileExistsError(f"Output already exists; explicit recovery is required: {path}")
    paths.writing_dir.mkdir()
    paths.vdif_writing_dir.mkdir()
    paths.tsys_dir.mkdir()


def stage_vdif_product(paths: VlbiProductPaths) -> Path:
    """Hard-link full-capture shards into unpublished output, preserving raw input.

    Both paths must be on the same filesystem. Shards are immutable after capture;
    future VDIF filtering must write new files, never overwrite these links.
    """
    manifest = read_capture_manifest(paths)
    for shard in manifest["shards"]:
        os.link(paths.vdif_dir / shard["name"], paths.vdif_writing_dir / shard["name"])
    return paths.vdif_writing_dir


def finalise_products(paths: VlbiProductPaths) -> None:
    """Publish VDIF first and its companion product last, with metadata in place.

    These are two atomic renames, not a transaction across both products. A failure
    between them leaves explicit incomplete state for subsequent recovery work.
    """
    for path in (paths.final_vdif_dir, paths.final_dir):
        if path.exists() or path.is_symlink():
            raise FileExistsError(f"Refusing to overwrite published product: {path}")
    for path in (paths.vdif_writing_dir, paths.writing_dir):
        if not (path / "metadata.json").is_file():
            raise FileNotFoundError(f"Cannot publish product without metadata: {path}")
    paths.vdif_writing_dir.rename(paths.final_vdif_dir)
    paths.writing_dir.rename(paths.final_dir)


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
        if path.is_file() and re.fullmatch(r".+\.[0-9]+", path.name)
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
