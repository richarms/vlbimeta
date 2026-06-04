# vlbimeta

`vlbimeta` is the online VLBI post-processing product for `katsdpcontroller`.

It replaces the older offline metadata notebooks and scripts. It is expected to
run as a scheduled postprocess task after `vlbi.<stream>` has finished recording,
on the same host as the recorder, with access to the local recorder output and
the live capture-block telstate.

## Role

`vlbimeta` owns the science-facing VLBI product assembly after capture.

It should:

- consume the completed or staged VDIF recorder product for one capture block
- consume observation metadata and sensor histories from capture-block telstate
- consume calibration products from the calibrated SDP stream when `antab` is enabled
- finalise product directories and write ingest/product metadata
- generate ANTAB, UVFLG, observation-log, and scan-manifest products
- eventually rewrite the raw full-session VDIF recording into a science-only VDIF product

It should not:

- expect historical raw beam-voltage files to exist
- download archived RDB files as the normal production path
- query the CAM archive as its normal production source of truth
- silently fall back to full-session pass-through when a requested science product cannot be made

The online contract is important: if a product needs a time history, that history
must either already be present in capture-block telstate or be mirrored there
during capture.

## Product Census

### Product finalisation

Input:

- `<data_dir>/<cbid>_vdif.writing` or `<data_dir>/<cbid>_vdif`
- capture-block `obs_params`
- stream name, normally `sdp_vdif`

Output:

- final `<data_dir>/<cbid>_vdif`
- `<data_dir>/<cbid>_vdif/metadata.json`
- `<data_dir>/<cbid>_antab/metadata.json`

### Scan manifest

The scan manifest should become the common selection contract for ANTAB, UVFLG,
observation logs, and science-only VDIF rewriting.

Input:

- scan start and stop times
- target names
- scan tags or classes
- catalogue metadata for provenance and cross-checking

Output:

- `scan_manifest.json`

The first manifest schema should stay small:

- `scan_id`
- `start_time`
- `end_time`
- `target_name`
- `tags`
- `include`
- `reason`

### ANTAB

ANTAB generation converts VLBI mean-power histories into calibrated Tsys-style
ANTAB output.

Input:

- per-thread mean-power histories, currently expected as:
  - `sdp_vdif.x0.mean-power`
  - `sdp_vdif.y0.mean-power`
  - `sdp_vdif.x1.mean-power`
  - `sdp_vdif.y1.mean-power`
- calibration products from the calibrated stream, normally `sdp_l0`
  - `cal_pol_ordering`
  - `cal_product_G` or `<cbid>_cal_product_G`
  - `product_B_parts` / `cal_product_B_parts`
  - `product_B0`, `product_B1`, ... or `<cbid>_cal_product_B0`, ...
  - antenna names from katdal or `bls_ordering`
  - calibrated stream frequency grid
- catalogue or equivalent structured metadata:
  - experiment name
  - scan names and scan windows
  - target names
  - channel centre frequencies, sidebands, bandwidths, and polarisation mapping
- station metadata:
  - station code
  - RXG data for DPFU, gain, Tcal, Trec, and spillover

Output:

- `<experiment><station>.antab`
- per-scan Tsys CSV files
- ANTAB provenance in `metadata.json`

### UVFLG

UVFLG generation is the online replacement for the flagging part of
`ipynb/log_uvflg_gen_rev2.ipynb`.

Input:

- scan manifest or catalogue-derived science scan windows
- expected target for each selected scan
- antenna list
- per-antenna activity histories, currently `<ant>_activity`
- per-antenna target histories, currently `<ant>_target`
- station code
- quorum policy, currently `0.90`
- flag time resolution, currently `1 s`

Output:

- `<experiment><station>.uvflg`

The notebook marked data invalid when fewer than the quorum of antennas reported
`track`, or when fewer than the quorum of antennas reported the expected target.
The production implementation should preserve that policy, but should consume
online telstate histories rather than the CAM archive.

### Observation log

Observation-log generation is the online replacement for the log part of
`ipynb/log_uvflg_gen_rev2.ipynb`.

Input:

- observation script log history, currently `obs_script_log`
- scan/log time axis from the manifest
- time-reference sensor histories, currently `tfrmon_tfr_ktt_utcza`
- sensor reporting interval, currently `60 s`

Output:

- `<experiment><station>.log`

### Science-only VDIF

The recorder captures the full VLBI session. The final delivered science VDIF
should eventually be rewritten from that raw staging output using the scan
manifest.

Input:

- raw VDIF staging directory
- scan manifest
- VDIF frame timestamps

Output:

- science-only VDIF product
- provenance linking the raw staging product to the final science product

The first implementation should use `baseband` for correctness before optimising
large-file throughput.

## Controller Entrypoint

The runtime entrypoint expected by `katsdpcontroller` is:

`vlbimeta.py <data_dir> <capture_block_id> <stream_name> [--mode antab|pass_through|disabled]`

Current behaviour:

- resolves capture/product directories from `data_dir`
- accepts `--telstate` from `katsdpcontroller`
- derives experiment metadata from telstate `obs_params`
- supports explicit modes:
  - `antab`
  - `pass_through`
  - `disabled`
- in `pass_through` mode:
  - finalises `<cbid>_vdif.writing` to `<cbid>_vdif`
  - writes `<cbid>_vdif/metadata.json` as a first-pass DLM `VDIFProduct`
  - writes `<cbid>_antab/metadata.json`
  - exits successfully without generating calibrated `ANTAB`

Current preferred layout is top-level under `data_dir`:

- `<data_dir>/<cbid>_vdif.writing`
- `<data_dir>/<cbid>_vdif`
- `<data_dir>/<cbid>_antab.writing`
- `<data_dir>/<cbid>_antab`

`vlbimeta` still accepts the older nested `<data_dir>/<cbid>/...` layout for
compatibility with earlier captures.

## Current State

Implemented:

- controller-facing `vlbimeta` entrypoint
- `pass_through` finalisation path
- metadata-only product output for `pass_through`
- telstate materialisation of the per-observation catalogue when present
- online telstate input helpers for future UVFLG/log products
- pure UVFLG evaluation and rendering from scan manifest plus antenna histories
- ANTAB prototype using telstate mean-power and calibrated-stream products
- fallback/debug tools for recomputing power from VDIF

Still to clean up:

- split notebook-derived logic into package modules instead of interactive scripts
- make the scan manifest the shared selection contract
- wire UVFLG and observation-log generation into the controller runtime
- define strict failure behaviour for missing online histories
- remove packaged catalogues from the production path
- align `metadata.json`, ANTAB, UVFLG, logs, and future VDIF filtering around the same scan selection

## Source Structure Direction

The package should be cleaned up around product responsibilities:

- `controller_entrypoint.py`: argument parsing, product orchestration, failure policy
- `runtime.py`: filesystem layout, metadata helpers, telstate materialisation
- `catalogue.py`: VLBI catalogue parsing and validation
- `manifest.py`: scan manifest generation and selection policy
- `antab.py`: ANTAB generation from mean-power, calibration, and scan metadata
- `uvflg.py`: UVFLG generation from antenna activity/target histories
- `obslog.py`: observation-log extraction and sensor-log insertion
- `vdif_rewrite.py`: science-only VDIF rewrite from manifest windows

The old notebooks under `ipynb/` should be treated as reference material for
algorithms and output formats, not as production source.

## Development Environment

Use a package-local virtual environment for development and tests:

```bash
UV_CACHE_DIR=.uv-cache uv venv .venv
UV_CACHE_DIR=.uv-cache uv pip install -e '.[test]'
.venv/bin/python -m pytest
```

The local `.venv/`, `.uv-cache/`, and generated `*.egg-info/` metadata are
ignored by git.

## Console Entry Points

Python code lives under `src/vlbimeta/` and is installable via `pyproject.toml`.
Console entry points currently include:

- `vlbimeta` / `vlbimeta.py`
- `vdif-power-summary`
- `vdif-power-antab`
- `telstate-antab-from-mean-power`

The controller-facing `vlbimeta` entrypoint is the production path. The other
entry points are currently fallback, debugging, or migration aids.
