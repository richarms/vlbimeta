"""Online telstate input helpers for VLBI metadata products."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence


ANTENNA_ACTIVITY_KEY_TEMPLATE = "{antenna}_activity"
ANTENNA_TARGET_KEY_TEMPLATE = "{antenna}_target"
OBSERVATION_SCRIPT_LOG_KEY = "obs_script_log"
TIME_REFERENCE_KEY = "tfrmon_tfr_ktt_utcza"


@dataclass(frozen=True)
class TelstateHistory:
    """A timestamped telstate history loaded from one key."""

    key: str
    samples: tuple[tuple[Any, float], ...]

    @property
    def values(self) -> tuple[Any, ...]:
        return tuple(value for value, _ in self.samples)

    @property
    def timestamps(self) -> tuple[float, ...]:
        return tuple(timestamp for _, timestamp in self.samples)


def format_antenna_key(template: str, antenna: str) -> str:
    return template.format(antenna=antenna)


def load_history(telstate: Any, key: str, *, required: bool = True) -> TelstateHistory | None:
    """Load a non-empty timestamped history from capture-block telstate."""
    if key not in telstate:  # type: ignore[operator]
        if required:
            raise KeyError(f"Required telstate history '{key}' is missing")
        return None
    samples = telstate.get_range(key, st=0)  # type: ignore[attr-defined]
    if not samples:
        if required:
            raise RuntimeError(f"Required telstate history '{key}' has no samples")
        return None
    return TelstateHistory(key=key, samples=tuple(samples))


def load_histories(
    telstate: Any,
    keys: Iterable[str],
    *,
    required: bool = True,
) -> dict[str, TelstateHistory]:
    histories = {}
    for key in keys:
        history = load_history(telstate, key, required=required)
        if history is not None:
            histories[key] = history
    return histories


def load_antenna_histories(
    telstate: Any,
    antennas: Sequence[str],
    *,
    key_template: str,
    required: bool = True,
) -> dict[str, TelstateHistory]:
    histories = {}
    for antenna in antennas:
        key = format_antenna_key(key_template, antenna)
        history = load_history(telstate, key, required=required)
        if history is not None:
            histories[antenna] = history
    return histories


def load_antenna_activity_histories(
    telstate: Any,
    antennas: Sequence[str],
    *,
    key_template: str = ANTENNA_ACTIVITY_KEY_TEMPLATE,
    required: bool = True,
) -> dict[str, TelstateHistory]:
    return load_antenna_histories(telstate, antennas, key_template=key_template, required=required)


def load_antenna_target_histories(
    telstate: Any,
    antennas: Sequence[str],
    *,
    key_template: str = ANTENNA_TARGET_KEY_TEMPLATE,
    required: bool = True,
) -> dict[str, TelstateHistory]:
    return load_antenna_histories(telstate, antennas, key_template=key_template, required=required)


def load_time_reference_history(
    telstate: Any,
    *,
    key: str = TIME_REFERENCE_KEY,
    required: bool = True,
) -> TelstateHistory | None:
    return load_history(telstate, key, required=required)


def _normalise_log_lines(value: Any) -> tuple[str, ...]:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return tuple(line for line in value.splitlines() if line)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        lines = []
        for item in value:
            if isinstance(item, bytes):
                item = item.decode("utf-8")
            if not isinstance(item, str):
                raise TypeError(f"Expected observation log line to be str or bytes, got {type(item)!r}")
            lines.append(item)
        return tuple(lines)
    raise TypeError(f"Expected observation script log to be str, bytes, or a sequence, got {type(value)!r}")


def load_observation_script_log(
    telstate: Any,
    *,
    key: str = OBSERVATION_SCRIPT_LOG_KEY,
    required: bool = True,
) -> tuple[str, ...] | None:
    """Load observation script log lines from capture-block telstate.

    The intended online contract is a scalar telstate value containing either a
    list of log lines or a newline-delimited string. This function also accepts
    a timestamped range of line values to make early integration less brittle.
    """
    if key not in telstate:  # type: ignore[operator]
        if required:
            raise KeyError(f"Required observation script log '{key}' is missing")
        return None

    try:
        value = telstate[key]  # type: ignore[index]
    except KeyError:
        samples = telstate.get_range(key, st=0)  # type: ignore[attr-defined]
        if not samples:
            if required:
                raise RuntimeError(f"Required observation script log '{key}' has no samples")
            return None
        return tuple(str(value) for value, _ in samples)

    lines = _normalise_log_lines(value)
    if required and not lines:
        raise RuntimeError(f"Required observation script log '{key}' is empty")
    return lines


def describe_online_input_keys(
    antennas: Sequence[str],
    *,
    activity_template: str = ANTENNA_ACTIVITY_KEY_TEMPLATE,
    target_template: str = ANTENNA_TARGET_KEY_TEMPLATE,
    observation_log_key: str = OBSERVATION_SCRIPT_LOG_KEY,
    time_reference_key: str = TIME_REFERENCE_KEY,
) -> dict[str, Any]:
    """Return the telstate keys a UVFLG/log run expects to consume."""
    return {
        "antenna_activity": {
            antenna: format_antenna_key(activity_template, antenna) for antenna in antennas
        },
        "antenna_target": {
            antenna: format_antenna_key(target_template, antenna) for antenna in antennas
        },
        "observation_script_log": observation_log_key,
        "time_reference": time_reference_key,
    }
