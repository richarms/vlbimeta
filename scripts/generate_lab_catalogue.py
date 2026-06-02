#!/usr/bin/env python3
"""Generate a short lab-specific VLBI catalogue for ANTAB plumbing tests.

This keeps the channel/header definition aligned with the packaged ES116A
catalogue, but emits a much shorter scan plan whose timestamps are near "now".
It is intended for lab image builds where `vlbimeta` still resolves catalogues
by experiment name.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


HEADER = """# EXPERIMENT {experiment_upper}
# CH01 1626.49 LSB 32.00 RCP
# CH02 1626.49 LSB 32.00 LCP
# CH03 1626.49 USB 32.00 RCP
# CH04 1626.49 USB 32.00 LCP
# POL XY
# CAL_PREFIX scan cal
"""


@dataclass(frozen=True)
class Scan:
    target_line: str
    scan_name: str
    start_offset: int
    duration: int


SCANS = (
    Scan(
        "J0408-6545 | PKS 0408-65 | PKS 0407-65, radec bfcal single_accumulation, "
        "04:08:20.37884, -65:45:09.0806, (850.0 1720.0 66.46 -62.7 20.46 -2.265)",
        "scan cal00",
        0,
        240,
    ),
    Scan(
        "*J1029A, radec, 10:29:13.9500000, 26:23:17.960000, ()",
        "scan No0001",
        300,
        60,
    ),
    Scan(
        "*1013+2449, radec, 10:13:53.4287720, 24:49:16.440790, ()",
        "scan No0002",
        420,
        120,
    ),
)


def _default_start_time() -> datetime:
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return now + timedelta(minutes=5)


def _format_scan(scan: Scan, start_time: datetime) -> str:
    scan_start = start_time + timedelta(seconds=scan.start_offset)
    return (
        f"{scan.target_line}, {scan.scan_name}, "
        f"{scan_start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}, {scan.duration}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--experiment",
        default="es116a_lab",
        help="Experiment identifier, used in the output filename [%(default)s].",
    )
    parser.add_argument(
        "--start-time",
        default=None,
        help="UTC start time in ISO format, e.g. 2026-04-16T12:00:00. "
        "Defaults to five minutes from now.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Write only to this explicit path instead of both repo catalogue locations.",
    )
    return parser.parse_args()


def _parse_start_time(raw: str | None) -> datetime:
    if raw is None:
        return _default_start_time()
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _catalogue_text(experiment: str, start_time: datetime) -> str:
    body = "\n".join(_format_scan(scan, start_time) for scan in SCANS)
    return HEADER.format(experiment_upper=experiment.upper()) + body + "\n"


def _repo_outputs(experiment: str) -> list[Path]:
    repo_root = Path(__file__).resolve().parents[1]
    filename = f"vlbi_cat_{experiment}.csv"
    return [
        repo_root / "catalogues" / filename,
        repo_root / "src" / "vlbimeta" / "catalogues" / filename,
    ]


def main() -> int:
    args = parse_args()
    experiment = args.experiment.strip().lower()
    start_time = _parse_start_time(args.start_time)
    outputs = [args.output] if args.output is not None else _repo_outputs(experiment)
    text = _catalogue_text(experiment, start_time)
    for output in outputs:
        assert output is not None
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
        print(f"wrote {output}")
    print(f"experiment={experiment}")
    print(f"start_time_utc={start_time.isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
