"""Scan manifest generation for VLBI post-processing products."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

from .catalogue import VlbiCatalogue, VlbiCatalogueScan
from .runtime import write_metadata_json


MANIFEST_VERSION = 2


@dataclass(frozen=True)
class ScanManifestEntry:
    scan_id: str
    start_time: datetime
    end_time: datetime
    target_name: str
    tags: tuple[str, ...]
    include: bool
    reason: str
    schedule_scan_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "scan_id": self.scan_id,
            "start_time": _iso_datetime_z(self.start_time),
            "end_time": _iso_datetime_z(self.end_time),
            "target_name": self.target_name,
            "tags": list(self.tags),
            "include": self.include,
            "reason": self.reason,
            "schedule_scan_id": self.schedule_scan_id,
            "execution_status": "unknown",
        }


def _iso_datetime_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def schedule_scan_id(scan_name: str) -> str | None:
    """Extract the original ID under the supplied CSV 'scan NoNNNN' convention.

    This is a CSV label mapping, not proof of membership in a particular VEX.
    Local calibration labels have no network scan ID. Preserve identifier case.
    """
    match = re.fullmatch(r"(?i:scan)\s+([Nn][Oo][0-9]+)", scan_name)
    return match[1] if match else None


def scan_class(scan_name: str, obs_params: Mapping[str, Any]) -> str:
    lowered = scan_name.lower()
    cal_prefix = str(obs_params.get("CAL_PREFIX") or "").lower()
    pcc_prefix = str(obs_params.get("PCC_PREFIX") or "").lower()
    if cal_prefix and lowered.startswith(cal_prefix):
        return "local_calibration"
    if pcc_prefix and lowered.startswith(pcc_prefix):
        return "local_phase_up"
    if schedule_scan_id(scan_name) is not None:
        return "scheduled"
    return "other"


def classify_scan(scan: VlbiCatalogueScan, obs_params: Mapping[str, Any]) -> tuple[tuple[str, ...], bool, str]:
    tag = scan_class(scan.name, obs_params)
    if tag == "scheduled":
        return (tag,), True, "scheduled VLBI scan from CSV"
    if tag == "local_calibration":
        return (tag,), False, "local calibration input; not a network scan"
    if tag == "local_phase_up":
        return (tag,), False, "local phase-up operation; not a network scan"
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
                schedule_scan_id=schedule_scan_id(scan.name) if include else None,
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
        "timing_authority": "csv_observation_plan",
        "scan_id_origin": "csv_scan_label",
        "selection_applied_to_vdif": False,
        "generated_utc": _iso_datetime_z(generated),
        "scans": [entry.to_dict() for entry in entries],
    }


def write_scan_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    write_metadata_json(path, manifest)
