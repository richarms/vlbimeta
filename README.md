# vlbimeta

VLBI post-processing utilities plus a controller-facing entrypoint for `katsdpcontroller`.

## Intended Role

`vlbimeta` is the scheduled post-processing step for VLBI captures.

The intended production path is:

1. `katgpucbf` V-engine produces the VLBI voltage stream and computes per-thread mean power during capture.
2. Those mean-power values are persisted in telstate.
3. `vlbimeta` runs after `vlbi.sdp_vdif` completes, reads the completed capture product plus telstate metadata, and writes ingest-facing / science-facing metadata products.

Important clarification:

- the old standalone workflow generated `vdif` and power outputs from saved raw beam-voltage files
- that is no longer the intended operational model here
- `vlbimeta` should not expect raw beam-voltage files to be present in postprocessing
- the primary power source for MVP work is telstate mean-power written during capture

## MVP Scope

Current MVP target:

- consume the completed VLBI capture product for one capture block
- read mean-power from telstate as the primary source
- generate `antab`
- generate per-product `metadata.json`

Implemented local-development path:

- `vlbimeta` can also run in `pass_through` mode
- this finalises the VDIF product directory and writes a metadata-only postprocess product
- it is intended for environments where calibration is not available

Deferred for now:

- `uvflag`
- observation log extraction
- any dependency on historical raw beam-voltage files

Fallback / debug path:

- power can also be recomputed from final VDIF voltage samples
- this is useful for validation or recovery
- it is not the primary operational source of truth

## Calibration Requirement

The current `antab` implementation assumes MeerKAT tied-array calibration
products are available.

In practice this means:

- telstate mean-power on its own is not enough
- `vlbimeta` expects calibration products equivalent to `katsdpcal` output
- the localhost sandbox `sim_vlbi_local.cfg` path does not currently launch `katsdpcal`
- therefore localhost can validate task plumbing and `pass_through`, but not calibrated `ANTAB`

Recommended operational policy:

- `antab` mode should require calibration and fail clearly if it is absent
- `pass_through` mode is the intended local development fallback

## Controller Entrypoint (Current Contract)

The container/runtime entrypoint expected by `katsdpcontroller` is:

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
  - writes `<cbid>_antab/metadata.json`
  - exits successfully without generating calibrated `ANTAB`

Remaining `antab`-mode contract work:

- the canonical mean-power sensor naming / thread ordering
- the calibration source contract for tied-array VLBI
- the final success/failure policy when calibration is absent

## Package Layout

Python code lives under `src/vlbimeta/` and is installable via `pyproject.toml`.
Console entry points are provided for:

- `vlbimeta` / `vlbimeta.py` (controller-facing product)
- `vdif-power-summary`
- `vdif-power-antab`
- `telstate-antab-from-mean-power`

Current status of these entry points:

- `vlbimeta` is the scheduled controller entrypoint and supports a metadata-only `pass_through` path
- `telstate-antab-from-mean-power` is the closest prototype to the intended calibrated `ANTAB` data path
- `vdif-power-summary` and `vdif-power-antab` remain useful as fallback/debug tooling and for validation against telstate-derived power
