"""Scan manifest generation for VLBI post-processing products."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from .catalogue import VlbiCatalogue, VlbiCatalogueScan
from .runtime import write_metadata_json


MANIFEST_VERSION = 1


@dataclass(frozen=True)
class ScanManifestEntry:
    scan_id: str
    start_time: datetime
    end_time: datetime
    target_name: str
    tags: tuple[str, ...]
    include: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "scan_id": self.scan_id,
            "start_time": _iso_datetime_z(self.start_time),
            "end_time": _iso_datetime_z(self.end_time),
            "target_name": self.target_name,
            "tags": list(self.tags),
            "include": self.include,
            "reason": self.reason,
        }


def _iso_datetime_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def scan_class(scan_name: str, obs_params: Mapping[str, Any]) -> str:
    lowered = scan_name.lower()
    cal_prefix = str(obs_params.get("CAL_PREFIX", "")).lower()
    pcc_prefix = str(obs_params.get("PCC_PREFIX", "")).lower()
    if cal_prefix and lowered.startswith(cal_prefix):
        return "calibrator"
    if pcc_prefix and lowered.startswith(pcc_prefix):
        return "pcc"
    if lowered.startswith("scan no"):
        return "science"
    return "other"


def classify_scan(scan: VlbiCatalogueScan, obs_params: Mapping[str, Any]) -> tuple[tuple[str, ...], bool, str]:
    tag = scan_class(scan.name, obs_params)
    if tag == "science":
        return (tag,), True, "science scan"
    if tag == "calibrator":
        return (tag,), False, "excluded calibrator scan"
    if tag == "pcc":
        return (tag,), False, "excluded phase-correction scan"
    return (tag,), False, "excluded unclassified scan"


def manifest_entries_from_catalogue(catalogue: VlbiCatalogue) -> list[ScanManifestEntry]:
    entries = []
    for scan in catalogue.scans:
        tags, include, reason = classify_scan(scan, catalogue.obs_params)
        entries.append(
            ScanManifestEntry(
                scan_id=scan.name,
                start_time=scan.start_time,
                end_time=scan.stop_time,
                target_name=scan.target,
                tags=tags,
                include=include,
                reason=reason,
            )
        )
    return entries


def build_scan_manifest(
    entries: Sequence[ScanManifestEntry],
    *,
    source: str,
    generated_utc: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_utc or datetime.now(timezone.utc)
    return {
        "version": MANIFEST_VERSION,
        "source": source,
        "generated_utc": _iso_datetime_z(generated),
        "scans": [entry.to_dict() for entry in entries],
    }


def write_scan_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    write_metadata_json(path, manifest)
