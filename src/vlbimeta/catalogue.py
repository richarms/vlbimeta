"""VLBI catalogue parsing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import katpoint


@dataclass(frozen=True)
class VlbiCatalogueScan:
    name: str
    target: str
    start_time: datetime
    duration: float
    target_description: str
    kp_target: katpoint.Target
    proc_start_time: datetime
    proc_duration: float

    @property
    def start_ts(self) -> float:
        return self.start_time.timestamp()

    @property
    def stop_time(self) -> datetime:
        return self.start_time + timedelta(seconds=self.duration)

    @property
    def stop_ts(self) -> float:
        return self.stop_time.timestamp()

    @property
    def proc_start_ts(self) -> float:
        return self.proc_start_time.timestamp()


@dataclass(frozen=True)
class VlbiCatalogue:
    obs_params: dict[str, Any]
    scans: tuple[VlbiCatalogueScan, ...]
    katpoint_catalogue: katpoint.Catalogue


def _parse_start_time(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)


def parse_vlbi_catalogue(
    path: str | Path,
    proc_buffer_sec: float = 1,
    ref_ant: katpoint.Antenna | None = None,
) -> VlbiCatalogue:
    """Parse a MeerKAT VLBI CSV catalogue.

    The current catalogue format carries both scan timing and ANTAB channel
    metadata. This parser preserves all header values rather than hard-coding a
    fixed header set so later products can decide which fields they require.
    """
    catalogue_path = Path(path)
    csv_lines = catalogue_path.read_text(encoding="utf-8").splitlines()
    obs_params: dict[str, Any] = {
        "EXPERIMENT": None,
        "POL": None,
        "CAL_PREFIX": None,
        "CHANNELS": {},
    }
    scans: list[VlbiCatalogueScan] = []

    for raw_line in csv_lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            header = line[1:].strip()
            if not header:
                continue
            header_key, *header_parts = header.split()
            if header_key.startswith("CH"):
                obs_params["CHANNELS"][header_key] = header_parts
            elif header_parts:
                obs_params[header_key] = " ".join(header_parts)
            continue

        parts = [part.strip() for part in raw_line.split(",")]
        if len(parts) < 7:
            raise ValueError(f"Invalid VLBI catalogue row in {catalogue_path}: {raw_line!r}")
        scan_name = parts[-3]
        start_time = _parse_start_time(parts[-2])
        duration = float(parts[-1])
        target = parts[0].split("|")[0].strip().lstrip("*")
        target_description = ", ".join(parts[:-3])
        proc_start_time = start_time - timedelta(seconds=proc_buffer_sec)
        scans.append(
            VlbiCatalogueScan(
                name=scan_name,
                target=target,
                start_time=start_time,
                duration=duration,
                target_description=target_description,
                kp_target=katpoint.Target(target_description, antenna=ref_ant),
                proc_start_time=proc_start_time,
                proc_duration=duration + 2 * proc_buffer_sec,
            )
        )

    scans.sort(key=lambda scan: scan.start_ts)
    kp_targets = []
    for scan in scans:
        if scan.kp_target not in kp_targets:
            kp_targets.append(scan.kp_target)
    return VlbiCatalogue(
        obs_params=obs_params,
        scans=tuple(scans),
        katpoint_catalogue=katpoint.Catalogue(kp_targets, antenna=ref_ant),
    )


def legacy_scan_dict(catalogue: VlbiCatalogue) -> dict[str, dict[str, Any]]:
    """Return the historical scan dictionary shape used by ANTAB code."""
    scan_params: dict[str, dict[str, Any]] = {}
    for scan in catalogue.scans:
        scan_params[scan.name] = {
            "target": scan.target,
            "start_iso": scan.start_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            "start_ts": scan.start_ts,
            "duration": int(scan.duration) if scan.duration.is_integer() else scan.duration,
            "kp_tgt": scan.kp_target,
            "proc_start_iso": scan.proc_start_time.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "proc_start_ts": scan.proc_start_ts,
            "proc_duration": scan.proc_duration,
        }
    return scan_params
