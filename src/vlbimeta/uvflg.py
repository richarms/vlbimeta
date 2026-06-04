"""UVFLG generation from online antenna activity and target histories."""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from .manifest import ScanManifestEntry
from .telstate_inputs import TelstateHistory


DEFAULT_QUORUM = 0.90
DEFAULT_INTERVAL = 1.0
DEFAULT_VALID_ACTIVITY = "track"
DEFAULT_REASON = "Antenna off source"


@dataclass(frozen=True)
class UvflgSample:
    timestamp: float
    expected_target: str
    activity_count: int
    target_count: int
    data_valid: bool


@dataclass(frozen=True)
class FlagRange:
    start_time: float
    end_time: float


@dataclass(frozen=True)
class UvflgEvaluation:
    antennas: tuple[str, ...]
    threshold: int
    samples: tuple[UvflgSample, ...]
    flag_ranges: tuple[FlagRange, ...]


def included_scans(entries: Sequence[ScanManifestEntry]) -> tuple[ScanManifestEntry, ...]:
    """Return scans selected for science products."""
    return tuple(entry for entry in entries if entry.include)


def build_scan_time_axis(
    entries: Sequence[ScanManifestEntry],
    *,
    interval: float = DEFAULT_INTERVAL,
) -> tuple[tuple[float, ...], tuple[str, ...]]:
    """Build the per-sample time axis and expected target names for UVFLG.

    Each included scan contributes whole sample timestamps inside the scan
    window. The timestamp list and target list have matching order and length.
    """
    if interval <= 0:
        raise ValueError("UVFLG interval must be positive")

    timestamps: list[float] = []
    expected_targets: list[str] = []
    for scan in included_scans(entries):
        start_ts = _timestamp_utc(scan.start_time)
        end_ts = _timestamp_utc(scan.end_time)
        if end_ts < start_ts:
            raise ValueError(f"Scan '{scan.scan_id}' ends before it starts")

        sample = math.ceil(start_ts / interval) * interval
        last_sample = math.floor(end_ts / interval) * interval
        while sample <= last_sample:
            timestamps.append(float(sample))
            expected_targets.append(normalise_target_name(scan.target_name))
            sample += interval

    if not timestamps:
        raise ValueError("No included scan samples available for UVFLG generation")
    return tuple(timestamps), tuple(expected_targets)


def evaluate_uvflg(
    entries: Sequence[ScanManifestEntry],
    activity_histories: Mapping[str, TelstateHistory],
    target_histories: Mapping[str, TelstateHistory],
    *,
    quorum: float = DEFAULT_QUORUM,
    interval: float = DEFAULT_INTERVAL,
    valid_activity: str = DEFAULT_VALID_ACTIVITY,
) -> UvflgEvaluation:
    """Evaluate UVFLG validity from included scans and antenna histories."""
    if not 0 < quorum <= 1:
        raise ValueError("UVFLG quorum must be greater than 0 and no more than 1")

    antennas = _validate_history_sets(activity_histories, target_histories)
    timestamps, expected_targets = build_scan_time_axis(entries, interval=interval)
    threshold = math.ceil(len(antennas) * quorum)

    samples = []
    for timestamp, expected_target in zip(timestamps, expected_targets):
        activity_count = 0
        target_count = 0
        for antenna in antennas:
            activity = nearest_history_value(activity_histories[antenna], timestamp)
            target = nearest_history_value(target_histories[antenna], timestamp)
            if activity == valid_activity:
                activity_count += 1
            if normalise_target_name(target) == expected_target:
                target_count += 1
        samples.append(
            UvflgSample(
                timestamp=timestamp,
                expected_target=expected_target,
                activity_count=activity_count,
                target_count=target_count,
                data_valid=activity_count >= threshold and target_count >= threshold,
            )
        )

    ranges = flag_ranges_from_samples(samples, interval=interval)
    return UvflgEvaluation(
        antennas=antennas,
        threshold=threshold,
        samples=tuple(samples),
        flag_ranges=ranges,
    )


def nearest_history_value(history: TelstateHistory, timestamp: float) -> Any:
    """Return the history value nearest to ``timestamp``."""
    if not history.samples:
        raise ValueError(f"Telstate history '{history.key}' has no samples")

    timestamps = history.timestamps
    index = bisect.bisect_left(timestamps, timestamp)
    if index == 0:
        return history.samples[0][0]
    if index == len(timestamps):
        return history.samples[-1][0]

    previous_ts = timestamps[index - 1]
    next_ts = timestamps[index]
    if timestamp - previous_ts <= next_ts - timestamp:
        return history.samples[index - 1][0]
    return history.samples[index][0]


def normalise_target_name(value: Any) -> str:
    """Return a comparable target name from a katpoint or sensor target value."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""

    try:
        import katpoint

        return str(katpoint.Target(text).name).strip().lstrip("*")
    except Exception:
        name = text.split(",", 1)[0].split("|", 1)[0]
        return name.strip().lstrip("*")


def flag_ranges_from_samples(
    samples: Sequence[UvflgSample],
    *,
    interval: float = DEFAULT_INTERVAL,
) -> tuple[FlagRange, ...]:
    """Compress consecutive invalid samples into UVFLG flag ranges."""
    if interval <= 0:
        raise ValueError("UVFLG interval must be positive")

    ranges = []
    current_start: float | None = None
    current_end: float | None = None

    for sample in samples:
        if sample.data_valid:
            if current_start is not None and current_end is not None:
                ranges.append(FlagRange(current_start, current_end))
            current_start = None
            current_end = None
            continue

        if current_start is None:
            current_start = sample.timestamp
        elif current_end is not None and sample.timestamp > current_end + interval:
            ranges.append(FlagRange(current_start, current_end))
            current_start = sample.timestamp
        current_end = sample.timestamp

    if current_start is not None and current_end is not None:
        ranges.append(FlagRange(current_start, current_end))

    return tuple(ranges)


def render_uvflg_lines(
    station_code: str,
    experiment: str,
    flag_ranges: Sequence[FlagRange],
    *,
    interval: float = DEFAULT_INTERVAL,
    created_date: date | None = None,
    reason: str = DEFAULT_REASON,
) -> tuple[str, ...]:
    """Render UVFLG file lines."""
    station = station_code.upper()
    extracted = created_date or datetime.now(timezone.utc).date()
    lines = [
        f"! The following is flagging information for {station} in experiment {experiment},",
        f"! extracted on {extracted.isoformat()}.",
        "opcode='FLAG'",
        f"dtimrang = {_format_interval(interval)}   timeoff=0",
    ]
    for flag_range in flag_ranges:
        lines.append(
            f"ant_name='{station}' timerang={format_timerang(flag_range.start_time, flag_range.end_time)} "
            f"reason='{reason}' /"
        )
    return tuple(lines)


def generate_uvflg_lines(
    entries: Sequence[ScanManifestEntry],
    activity_histories: Mapping[str, TelstateHistory],
    target_histories: Mapping[str, TelstateHistory],
    *,
    station_code: str,
    experiment: str,
    quorum: float = DEFAULT_QUORUM,
    interval: float = DEFAULT_INTERVAL,
    valid_activity: str = DEFAULT_VALID_ACTIVITY,
    created_date: date | None = None,
    reason: str = DEFAULT_REASON,
) -> tuple[UvflgEvaluation, tuple[str, ...]]:
    """Evaluate histories and render the corresponding UVFLG lines."""
    evaluation = evaluate_uvflg(
        entries,
        activity_histories,
        target_histories,
        quorum=quorum,
        interval=interval,
        valid_activity=valid_activity,
    )
    lines = render_uvflg_lines(
        station_code,
        experiment,
        evaluation.flag_ranges,
        interval=interval,
        created_date=created_date,
        reason=reason,
    )
    return evaluation, lines


def format_timerang(start_time: float, end_time: float) -> str:
    """Format two UTC timestamps as a VLBI UVFLG ``timerang`` value."""
    start = _datetime_utc(start_time)
    end = _datetime_utc(end_time)
    return f"{_format_time_fields(start)}, {_format_time_fields(end)}"


def _validate_history_sets(
    activity_histories: Mapping[str, TelstateHistory],
    target_histories: Mapping[str, TelstateHistory],
) -> tuple[str, ...]:
    activity_antennas = set(activity_histories)
    target_antennas = set(target_histories)
    if not activity_antennas:
        raise ValueError("No antenna activity histories supplied for UVFLG generation")
    if activity_antennas != target_antennas:
        missing_activity = sorted(target_antennas - activity_antennas)
        missing_target = sorted(activity_antennas - target_antennas)
        details = []
        if missing_activity:
            details.append(f"missing activity for {missing_activity}")
        if missing_target:
            details.append(f"missing target for {missing_target}")
        raise ValueError("Antenna activity and target history sets differ: " + "; ".join(details))
    return tuple(sorted(activity_antennas))


def _timestamp_utc(value: datetime) -> float:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).timestamp()


def _datetime_utc(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp, timezone.utc)


def _format_time_fields(value: datetime) -> str:
    return f"{value.timetuple().tm_yday:03d},{value.hour:02d},{value.minute:02d},{value.second:02d}"


def _format_interval(interval: float) -> str:
    if float(interval).is_integer():
        return str(int(interval))
    return str(interval)
