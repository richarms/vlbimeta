# vlbimeta

VLBI post-processing utilities plus a controller-facing entrypoint for `katsdpcontroller`.

## Controller Entrypoint (MVP)

The container/runtime entrypoint expected by `katsdpcontroller` is:

`vlbimeta.py <data_dir> <capture_block_id> <stream_name>`

Currently:

- validates `data_dir` exists and is a directory
- logs invocation parameters and resolved asset paths
- exits successfully (`0`)
- doesn't do anything real yet

## Package Layout

Python code lives under `src/vlbimeta/` and is installable via `pyproject.toml`.
Console entry points are provided for:

- `vlbimeta` / `vlbimeta.py` (controller-facing product)
- `vdif-power-summary`
- `vdif-power-antab`
- `telstate-antab-from-mean-power`

