# Original VEX input contract, version 1

Update, 2026-09-11: the [CSV observing plan](csv_observation_plan.md) is the
runtime authority. This VEX transport remains available for optional reference
and provenance; it is not required by the CSV/ANTAB path.

Implemented on `feat/vex-schedule-contract`. This contract transports the
original schedule and verifies its provenance. It does **not** yet validate VEX
syntax, station/mode references, scan timing, execution or VDIF selection. The
existing controller entrypoint still uses its CSV/full-capture path; this API
and CLI are the first independently testable VEX building block.

## Telstate value

Write one immutable `vlbi_schedule` value in the explicit capture-block view:

```python
capture = telstate.view(capture_block_id, exclusive=True)
capture.add("vlbi_schedule", {
    "version": 1,
    "capture_block_id": capture_block_id,
    "vex": {
        "name": original_basename,
        "content": original_bytes,
        "sha256": hashlib.sha256(original_bytes).hexdigest(),
    },
    "station": vex_station_identifier,
    "producer": {
        "name": "vlbi_online.py",
        "revision": script_revision,
    },
}, immutable=True)
```

This is a telstate dictionary, not JSON: `vex.content` must be Python `bytes`.
The consumer requires exactly these version-1 fields. The full key is
`<cbid>_vlbi_schedule`; neither `obs_vlbi_schedule` nor root-scoped values are
accepted. `exclusive=True` prevents fallback even if the caller supplies a view
that already contains other capture or root prefixes.

The capture ID is a decimal string, at most 128 characters. The VEX content is
non-empty and at most 16 MiB. Read in binary mode without stripping whitespace,
rewriting newlines or changing case. The digest is mandatory lowercase SHA-256
over those bytes. A checksum identifies content; it does not authenticate its
origin or prove that the schedule was executed.

`vex.name` is a basename (up to 255 characters), used only for provenance. It
never determines an output path. `station` is the exact VEX station identifier
(up to 128 characters), not an inferred export/VDIF station code. Producer name
and revision are non-empty strings, each up to 1024 characters. Text fields
cannot contain control characters. Semantic station validation follows in the
VEX interpretation stage.

Publishing the same value again is idempotent. Changing any field under the
same capture ID fails. A mutable key, wrong embedded capture ID, unsupported
contract version, malformed value or changed content/digest fails consumption.
There is no fallback to CSV or packaged files in this API.

## Observation-script integration

Preflight/read the source file before starting the observation; publish after
the authoritative capture ID is available and before the first scheduled scan.
The current copied katcorelib script calls its CSV publisher just after
`session.capture_start()`. Confirm this placement against the maintained
script's actual session lifecycle. Do not guess a capture ID, suppress view
errors or fall back to a root view.

The tested helper can be used directly in a lab driver:

```python
from pathlib import Path
from vlbimeta.vex_schedule import VexSchedule, publish_schedule, read_vex_file

vex_path = Path(options.vex)
content = read_vex_file(vex_path)  # bounded binary read before capture

# Once the session has provided the authoritative capture_block_id:
schedule = VexSchedule(
    capture_block_id=capture_block_id,
    name=vex_path.name,
    content=content,
    station=options.vex_station,
    producer_name="vlbi_online.py",
    producer_revision=script_revision,
)
publish_schedule(telstate, schedule)
```

The wire contract also permits the observation script to publish the dictionary
directly using its existing `katsdptelstate` dependency. It need not install the
science-processing stack just to construct that value. Producer/consumer tests
assert the literal key and dictionary shape as well as round-trip behavior.

This change does not modify the possibly outdated `mgouws_vlbi` script. Its
existing CSV publisher's `obs_` keys are incompatible with the current CSV
consumer. The VEX publisher uses the new, single namespace above. If execution
still uses converted CSV, retain converter provenance and an explicit mapping
to the original VEX scan IDs in the forthcoming execution contract. Merely
publishing a VEX does not establish that it matches the executed CSV.

## Input snapshot

`read_schedule(telstate, capture_block_id)` verifies the wire value.
`write_schedule_snapshot(schedule, output_dir)` saves:

```text
output_dir/
    schedule.vex
    schedule.json
```

`schedule.vex` contains the original bytes. `schedule.json` has the same outer
version, capture ID, station and producer fields, with the VEX field containing:

```json
{
  "name": "original.vex",
  "sha256": "<64 lowercase hexadecimal characters>",
  "content_file": "schedule.vex",
  "size_bytes": 1234
}
```

`read_schedule_snapshot(input_dir, capture_block_id)` verifies size, digest,
identity and schema without Redis. It requires these two regular files and
rejects symlinks or alternate content paths. This is an **input snapshot**,
not `scan_manifest.json` and not a published science product.

The writer requires a fresh destination and writes metadata last. It does not
overwrite or adopt existing directories. Partial snapshots fail verification;
automatic staging recovery and multi-product publication are later work.
The normal published VDIF/raw handoff is unchanged by this module.

## CLI

After installation, `vlbimeta-vex` is equivalent to
`python -m vlbimeta.vex_schedule`. From a source checkout, set `PYTHONPATH=src`.

On a **disposable lab Redis**, use its endpoint in place of `127.0.0.1:6379`:

```bash
vlbimeta-vex publish --telstate 127.0.0.1:6379 --capture-block-id 177 \
    --vex tests/fixtures/transport.vex --station Me \
    --producer lab-contract-test --producer-revision fixture-v1

vlbimeta-vex snapshot --telstate 127.0.0.1:6379 --capture-block-id 177 \
    --output-dir /tmp/vlbi-177-input

vlbimeta-vex verify --capture-block-id 177 --input-dir /tmp/vlbi-177-input
```

The commands print JSON provenance, without the VEX contents, and fail with a
nonzero exit code on contract errors. Successful verification means the input
transport/snapshot is consistent, not that the VEX is semantically valid.

## Tests and next gate

```bash
python -m pytest tests/test_vex_schedule.py
VLBIMETA_TEST_REDIS=127.0.0.1:6379 python -m pytest tests/test_vex_schedule.py
```

The first command runs the in-memory/pure tests and skips real Redis cases.
The second also exercises Redis serialization and immutable-key behavior.
Use a disposable Redis instance. Tests reserve only `vlbi_schedule`,
`obs_vlbi_schedule`, `177_vlbi_schedule`, `178_vlbi_schedule` and
`177_obs_vlbi_schedule`; they refuse existing test keys and clean up those keys.
They do not flush the database. These cases should run serially because the
literal namespaces are part of the assertions.

`tests/fixtures/transport.vex` is synthetic input for transport testing. It is
not a complete operational telescope configuration or a validated instrument
mapping. The tests also exercise CRLF and UTF-8 comment preservation. The next
gate is a supported VEX parser/profile with representative observing files,
station-specific intervals, mode/stream mapping, original scan identifiers and
independent expected manifests. Execution events/completion and all VDIF/science
processing remain separate subsequent work.
