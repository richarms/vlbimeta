# Canonical CSV observing plan

Implemented on `feat/csv-observation-plan`, 2026-09-11. The CSV used by the
observation script is the authority for the local observing plan: experiment,
selected scans, intended timings and local calibration operations. VEX remains
an optional reference/provenance asset. OPT/YAML will need an explicit adapter
when its schema is available.

## Producer and consumer contract

The production observing path is `katsdpscripts/observation/vlbi_online.py`, a
close copy of Marcel's existing observing and calibration procedure. The katobs
tracking-only prototype is retired; neither the runtime nor tests require it.
The katsdpscripts draft still needs product-specific capture selection and a
deployed-session test. See [the CAM smoke procedure](cam_smoke.md) for the
remaining integration prerequisites; this contract does not certify that draft
as ready for live execution.

The producer freezes the original UTF-8 CSV bytes before parsing the plan, then
publishes those same bytes once the capture ID is known, before the first scan.
All four keys below are required and immutable in
`telstate.view(capture_block_id, exclusive=True)`:

| Key | Value |
| --- | --- |
| `vlbi_catalogue_content` | Original bytes; non-empty, at most 16 MiB |
| `vlbi_catalogue_sha256` | Lowercase SHA-256 of the original bytes |
| `vlbi_catalogue_name` | Original basename, without path separators/control characters |
| `vlbi_catalogue_format` | Exactly `csv` |

These retain the existing direct `vlbi_catalogue_*` namespace. There is no
`obs_` prefix, root fallback or fallback to another capture. Consumers require
all four immutable values. Publication consists of multiple immutable writes;
it is not an atomic bundle. A failed/incomplete publication must fail the
observation's metadata handoff, and postprocessing rejects missing inputs.
Identical writes are idempotent; conflicting values raise an error.

Legacy string content is accepted with the digest computed from its UTF-8
encoding. New producers must read in binary mode to preserve original line
endings. Materialisation writes the verified bytes in binary mode without
normalization and refuses to overwrite an existing file.

The `katsdpscripts/observation/vlbi_online.py` draft freezes the
input before parsing and passes the frozen bytes to its publisher after
`capture_start`. Its helpers require a capture ID from the session or its SDP
capture-ID value; they no longer suppress capture-view errors. This is not yet
sufficient for sessions with separate wideband/narrowband products: select the
actual product containing the requested `sdp.vdif` output and publish into that
product's exclusive capture view. Recording currently starts at `capture_init`;
publish once the authoritative identity exists and before scheduled observing
work. Its `vlbi_experiment` key is supplementary:
the authoritative experiment identifier is the CSV's `EXPERIMENT` header.

## Runtime changes

In `antab` mode the controller entrypoint requires the capture-scoped CSV.
Packaged catalogues are no longer selected implicitly from a proposal ID.
The CSV's experiment identifier, including case, is used for generated metadata
and ANTAB filenames. `--experiment` may check that identifier (case-insensitively)
but cannot replace it with a different experiment. A telescope proposal ID is
not used to identify the VLBI experiment in this path.

An explicit `--catalogue /path/to/file.csv` is available for development. Relative
paths are resolved under `--catalogue-dir`. This override still archives and
hashes the input and records `catalogue_source=explicit_file`; it is never an
automatic recovery path. ANTAB still requires telstate for its other inputs.

A [reference patch](observation_csv.patch) against the supplied
`vlbi_observation.py` preserves the script changes with this repository. It
produces the behavior implemented in the supplied `vlbi_online.py` copy; check
it against the maintained script before applying there.

Deploy the matching producer before enabling this stricter ANTAB consumer.
Older helpers that omit the format or digest, or write `obs_` keys, will fail
clearly. The older kattelmod checkout has not been modified in this increment.
`pass_through` continues to operate without a CSV, and continues to hard-link
full-capture raw files. This increment does not duplicate VDIF payloads.

## Normalized manifest, version 2

The parser rejects missing/invalid experiment identity, empty catalogues,
duplicate labels/headers and non-finite or non-positive durations. It preserves
fractional timestamps, all supplied header values and the CSV's timings. It
does not add the VEX station offset or insert scans that the CSV omitted.

Each manifest entry retains the existing full CSV label as `scan_id` and adds
`schedule_scan_id`. Under the supplied CSV profile, `scan No0004` maps to
`No0004`, preserving case. The manifest records `scan_id_origin=csv_scan_label`:
this is a documented label convention, not a claim that a VEX was parsed or
that membership in a particular VEX has been verified.

Selection roles are:

- `scheduled`: supported `scan NoNNNN` entries, including network calibrators;
- `local_calibration`: entries identified by the CSV's `CAL_PREFIX`;
- `local_phase_up`: entries identified by `PCC_PREFIX`;
- `other`: unrecognized entries, retained with an exclusion reason.

Local operations remain available as metadata/calibration inputs and are not
assigned a network scan ID. ANTAB's scan selection uses the same classifier as
the manifest. Unknown naming conventions require an explicit mapping/adapter
before future scan export; do not guess IDs from target names or row order.

`include` denotes eligibility for future scan export. The manifest explicitly
records `selection_applied_to_vdif=false`; all execution statuses are `unknown`.
The CSV is scheduled intent, not evidence of successful tracking or calibration.
Frame coverage, execution events, quality flags and per-scan VDIF output are
subsequent work. Manifest schema 2 changes role tags and preserves fractional
time; downstream readers must account for this version change.

## Diagnostic CLI

`vlbimeta-plan` (also `python -m vlbimeta.observation_plan`) generates a verified
CSV snapshot and manifest without reading or copying any VDIF:

```bash
vlbimeta-plan --catalogue observation.csv --output-dir /tmp/vlbi-plan

vlbimeta-plan --telstate 127.0.0.1:6379 --capture-block-id 177 \
    --output-dir /tmp/vlbi-177-plan
```

Use a fresh output directory. The result is `catalogue/<original basename>` and
`scan_manifest.json`, with input digest, origin and experiment. Invalid input
returns a nonzero exit status and may leave the input snapshot for diagnosis;
no successful manifest is written. These are diagnostic files, not published
SDP science products. Existing output directories are not adopted or overwritten.

## Validation and next steps

The six supplied site CSVs are regression fixtures under
`tests/fixtures/observing_plans`. Expected selected/total entries are EG144 81/84,
EL080 366/371, ES123A 54/56, EX011A 108/111, GS047C 39/42 and N26L2 14/16.
Tests check exact bytes/digests, CSV timing, fractional boundaries, network
calibrator inclusion, namespace isolation, strict runtime inputs and provenance.

The observation parser and publisher can be tested without importing telescope
control code: tests load only their functions using the Python AST. They default
to the sibling katsdpscripts checkout. Set
`VLBI_OBSERVATION_SCRIPT=/path/to/vlbi_online.py` for a different location.
The parser comparison covers all six site CSVs and checks scan identities,
timings and targets. Publisher tests check exact bytes using a simplified
session; they do not validate real multi-product capture selection.
Set `VLBIMETA_TEST_REDIS=host:port` to a disposable Redis to also
exercise its real immutable storage. The CSV integration test reserves and
cleans only the four `177_vlbi_catalogue_*` keys above plus `177_vlbi_experiment`;
it refuses pre-existing test keys. Run these tests serially.

Next: complete the maintained producer's capture selection and deployment, add
execution/sensor histories, and integrate the measured index/export diagnostics
only after agreeing consumer requirements and recovery behaviour. Preserve raw
data and the existing handoff while establishing that evidence.
