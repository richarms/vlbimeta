"""Capture-scoped transport of original VEX bytes and reproducible input snapshots.

This module validates the transport contract, not VEX syntax or observing
semantics. Successful publication must not be interpreted as a valid schedule
or evidence that any scheduled scan was observed.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Sequence

import katsdptelstate
from katsdptelstate.errors import TelstateError


SCHEDULE_KEY = "vlbi_schedule"
SCHEDULE_VERSION = 1
MAX_VEX_BYTES = 16 * 1024 * 1024
MAX_METADATA_BYTES = 64 * 1024
SNAPSHOT_CONTENT = "schedule.vex"
SNAPSHOT_METADATA = "schedule.json"


def _text(value: Any, field: str, limit: int = 1024) -> None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > limit
        or any(ord(c) < 32 or ord(c) == 127 for c in value)
    ):
        raise ValueError(f"{field} must be non-empty text without control characters (max {limit})")


def _capture_id(value: Any) -> None:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,128}", value):
        raise ValueError("capture_block_id must be a non-empty decimal string (max 128)")


def _fields(value: Any, expected: set[str], field: str) -> None:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError(f"{field} must contain exactly these fields: {', '.join(sorted(expected))}")


def _content(value: Any) -> None:
    if not isinstance(value, bytes) or not 0 < len(value) <= MAX_VEX_BYTES:
        raise ValueError(f"vex.content must be 1..{MAX_VEX_BYTES} original bytes")


def read_vex_file(path: Path) -> bytes:
    """Read bounded binary input without newline or encoding normalization."""
    with path.open("rb") as stream:
        content = stream.read(MAX_VEX_BYTES + 1)
    _content(content)
    return content


@dataclass(frozen=True)
class VexSchedule:
    """Validated version-1 transport value; identifiers retain their exact case."""

    capture_block_id: str
    name: str
    content: bytes
    station: str
    producer_name: str
    producer_revision: str

    def __post_init__(self) -> None:
        _capture_id(self.capture_block_id)
        _content(self.content)
        _text(self.name, "vex.name", 255)
        if self.name in (".", "..") or "/" in self.name or "\\" in self.name:
            raise ValueError("vex.name must be a basename, not a path")
        _text(self.station, "station", 128)
        _text(self.producer_name, "producer.name")
        _text(self.producer_revision, "producer.revision")

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        """The single immutable telstate value (bytes are not JSON encoded)."""
        return {
            "version": SCHEDULE_VERSION,
            "capture_block_id": self.capture_block_id,
            "vex": {"name": self.name, "content": self.content, "sha256": self.sha256},
            "station": self.station,
            "producer": {"name": self.producer_name, "revision": self.producer_revision},
        }

    @classmethod
    def from_dict(cls, value: Any, *, capture_block_id: str) -> VexSchedule:
        """Check wire shape, capture identity, content type and mandatory digest."""
        _capture_id(capture_block_id)
        _fields(value, {"version", "capture_block_id", "vex", "station", "producer"}, "schedule")
        if type(value["version"]) is not int or value["version"] != SCHEDULE_VERSION:
            raise ValueError("Unsupported VLBI schedule contract version")
        if value["capture_block_id"] != capture_block_id:
            raise ValueError("VLBI schedule capture_block_id does not match requested capture")
        vex = value["vex"]
        _fields(vex, {"name", "content", "sha256"}, "vex")
        producer = value["producer"]
        _fields(producer, {"name", "revision"}, "producer")
        schedule = cls(capture_block_id, vex["name"], vex["content"], value["station"],
                       producer["name"], producer["revision"])
        if not isinstance(vex["sha256"], str) or vex["sha256"] != schedule.sha256:
            raise ValueError("VEX SHA-256 mismatch (lowercase digest of original bytes required)")
        return schedule

    def snapshot_metadata(self) -> dict[str, Any]:
        """JSON metadata for the input snapshot, not a derived scan manifest."""
        value = self.to_dict()
        del value["vex"]["content"]
        value["vex"].update(content_file=SNAPSHOT_CONTENT, size_bytes=len(self.content))
        return value


def publish_schedule(telstate: Any, schedule: VexSchedule) -> None:
    """Publish once in the explicit capture namespace.

    Repeating an identical value is permitted by telstate. Replacing it or
    using an existing mutable key raises an error. No session/capture discovery
    is performed: the caller must supply the actual capture identity.
    """
    view = telstate.view(schedule.capture_block_id, exclusive=True)
    view.add(SCHEDULE_KEY, schedule.to_dict(), immutable=True)


def read_schedule(telstate: Any, capture_block_id: str) -> VexSchedule:
    """Read only this capture's immutable value, even from an inherited view."""
    _capture_id(capture_block_id)
    view = telstate.view(capture_block_id, exclusive=True)
    key_type = view.key_type(SCHEDULE_KEY)
    if key_type is None:
        raise KeyError(f"Missing {capture_block_id}_{SCHEDULE_KEY}")
    if key_type != katsdptelstate.KeyType.IMMUTABLE:
        raise ValueError(f"{capture_block_id}_{SCHEDULE_KEY} must be immutable")
    return VexSchedule.from_dict(view[SCHEDULE_KEY], capture_block_id=capture_block_id)


def write_schedule_snapshot(schedule: VexSchedule, output_dir: Path) -> None:
    """Save original bytes and JSON provenance in a fresh, unpublished directory.

    Existing directories are never adopted or overwritten. Metadata is written
    last; a failed write leaves diagnostic staging for explicit recovery.
    """
    output_dir.mkdir(parents=True, exist_ok=False)
    with (output_dir / SNAPSHOT_CONTENT).open("xb") as stream:
        stream.write(schedule.content)
    with (output_dir / SNAPSHOT_METADATA).open("x", encoding="utf-8") as stream:
        json.dump(schedule.snapshot_metadata(), stream, indent=2, sort_keys=True)
        stream.write("\n")


def read_schedule_snapshot(input_dir: Path, capture_block_id: str) -> VexSchedule:
    """Verify a saved input independently of Redis, including its original hash."""
    if input_dir.is_symlink() or not input_dir.is_dir():
        raise ValueError("Schedule snapshot must be a regular directory")
    if {path.name for path in input_dir.iterdir()} != {SNAPSHOT_CONTENT, SNAPSHOT_METADATA}:
        raise ValueError("Schedule snapshot requires exactly schedule.vex and schedule.json")
    for name in (SNAPSHOT_CONTENT, SNAPSHOT_METADATA):
        path = input_dir / name
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Snapshot {name} must be a regular file")
    with (input_dir / SNAPSHOT_METADATA).open("rb") as stream:
        raw = stream.read(MAX_METADATA_BYTES + 1)
    if len(raw) > MAX_METADATA_BYTES:
        raise ValueError("Schedule snapshot metadata is too large")
    value = json.loads(raw)
    _fields(value, {"version", "capture_block_id", "vex", "station", "producer"}, "snapshot")
    vex = value["vex"]
    _fields(vex, {"name", "sha256", "size_bytes", "content_file"}, "snapshot.vex")
    if vex.pop("content_file") != SNAPSHOT_CONTENT:
        raise ValueError("Snapshot content_file must be schedule.vex")
    content = read_vex_file(input_dir / SNAPSHOT_CONTENT)
    size = vex.pop("size_bytes")
    if type(size) is not int or size != len(content):
        raise ValueError("Snapshot VEX size mismatch")
    vex["content"] = content
    return VexSchedule.from_dict(value, capture_block_id=capture_block_id)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    publish = commands.add_parser("publish", help="Publish original VEX bytes to capture telstate")
    publish.add_argument("--telstate", required=True)
    publish.add_argument("--capture-block-id", required=True)
    publish.add_argument("--vex", type=Path, required=True)
    publish.add_argument("--station", required=True)
    publish.add_argument("--producer", required=True)
    publish.add_argument("--producer-revision", required=True)
    snapshot = commands.add_parser("snapshot", help="Save a verified input snapshot from telstate")
    snapshot.add_argument("--telstate", required=True)
    snapshot.add_argument("--capture-block-id", required=True)
    snapshot.add_argument("--output-dir", type=Path, required=True)
    verify = commands.add_parser("verify", help="Verify a saved input snapshot without Redis")
    verify.add_argument("--capture-block-id", required=True)
    verify.add_argument("--input-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "publish":
            schedule = VexSchedule(args.capture_block_id, args.vex.name, read_vex_file(args.vex),
                                   args.station, args.producer, args.producer_revision)
            publish_schedule(katsdptelstate.TelescopeState(args.telstate), schedule)
        elif args.command == "snapshot":
            schedule = read_schedule(katsdptelstate.TelescopeState(args.telstate), args.capture_block_id)
            write_schedule_snapshot(schedule, args.output_dir)
        else:
            schedule = read_schedule_snapshot(args.input_dir, args.capture_block_id)
    except (ValueError, KeyError, OSError, TelstateError) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    print(json.dumps(schedule.snapshot_metadata(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
