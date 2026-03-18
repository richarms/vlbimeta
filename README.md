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

Deferred for now:

- `uvflag`
- observation log extraction
- any dependency on historical raw beam-voltage files

Fallback / debug path:

- power can also be recomputed from final VDIF voltage samples
- this is useful for validation or recovery
- it is not the primary operational source of truth

## Controller Entrypoint (Current Contract)

The container/runtime entrypoint expected by `katsdpcontroller` is:

`vlbimeta.py <data_dir> <capture_block_id> <stream_name>`

Currently:

- validates `data_dir` exists and is a directory
- logs invocation parameters and resolved asset paths
- exits successfully (`0`)
- doesn't do anything real yet

This contract is intentionally minimal today, but the MVP implementation will need to resolve:

- the telstate dataset for the completed capture block
- the canonical mean-power sensor naming / thread ordering
- the experiment / catalogue metadata required for `antab`
- the output product directory and `metadata.json` contract

## Package Layout

Python code lives under `src/vlbimeta/` and is installable via `pyproject.toml`.
Console entry points are provided for:

- `vlbimeta` / `vlbimeta.py` (controller-facing product)
- `vdif-power-summary`
- `vdif-power-antab`
- `telstate-antab-from-mean-power`

Current status of these entry points:

- `vlbimeta` is the scheduled controller entrypoint but is still a no-op
- `telstate-antab-from-mean-power` is the closest prototype to the intended MVP data path
- `vdif-power-summary` and `vdif-power-antab` remain useful as fallback/debug tooling and for validation against telstate-derived power
