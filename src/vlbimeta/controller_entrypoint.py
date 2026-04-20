"""Controller-facing `vlbimeta` entrypoint."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import logging
from pathlib import Path
from typing import Sequence

import katsdptelstate

from .paths import default_catalogue_dir, default_metadata_dir
from .runtime import (
    antab_product_paths,
    build_vdif_product_metadata,
    derive_experiment_name,
    finalise_vdif_dir,
    finalise_product_dir,
    materialise_catalogue_from_telstate,
    prepare_writing_dir,
    resolve_catalogue_path,
    write_metadata_json,
)
from .telstate_antab_from_mean_power import generate_antab_from_capture


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="vlbimeta.py",
        description="katsdpcontroller VLBI post-processing entrypoint.",
    )
    parser.add_argument("data_dir", type=Path, help="Shared capture volume mounted by katsdpcontroller.")
    parser.add_argument("capture_block_id", help="Capture block ID for the completed observation.")
    parser.add_argument("stream_name", help="VDIF stream name for this post-processing task.")
    parser.add_argument(
        "--dataset-stream-name",
        default=None,
        help="Telstate dataset stream name for calibration products (defaults to katdal's normal resolution).",
    )
    parser.add_argument(
        "--mode",
        choices=("antab", "pass_through", "disabled"),
        default="antab",
        help="vlbimeta execution mode [%(default)s].",
    )
    parser.add_argument(
        "--telstate",
        help="Telstate endpoint (host:port), passed by katsdpcontroller for capture-block postprocessing.",
    )
    parser.add_argument(
        "--name",
        default="vlbimeta",
        help="Task name supplied by katsdpcontroller [%(default)s].",
    )
    parser.add_argument(
        "--experiment",
        default=None,
        help="Override the VLBI experiment identifier (defaults to telstate obs_params.proposal_id).",
    )
    parser.add_argument(
        "--station-code",
        default="me",
        help="Two-character VLBI station code [%(default)s].",
    )
    parser.add_argument(
        "--sensor-pols",
        default="x,y",
        help="Comma-separated sensor polarisation labels matching V-engine mean-power sensors [%(default)s].",
    )
    parser.add_argument(
        "--rxg",
        type=Path,
        default=Path("calmel.rxg"),
        help="Path to the station RXG file [%(default)s].",
    )
    parser.add_argument(
        "--gain-tab",
        type=float,
        default=0.5,
        help="Assumed tied-array beam gain used when deriving Tsys [%(default)s].",
    )
    parser.add_argument(
        "--time-buffer",
        type=float,
        default=0.0,
        help="Seconds to expand each scan window by when selecting power samples [%(default)s].",
    )
    parser.add_argument(
        "--catalogue-dir",
        type=Path,
        default=None,
        help="Override catalogue directory (defaults to packaged assets or image layout).",
    )
    parser.add_argument(
        "--metadata-dir",
        type=Path,
        default=None,
        help="Override metadata directory (defaults to packaged assets or image layout).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level [%(default)s].",
    )
    return parser.parse_args(argv)


def _parse_sensor_pols(raw: str) -> tuple[str, str]:
    parts = tuple(part.strip() for part in raw.split(",") if part.strip())
    if len(parts) != 2:
        raise ValueError(f"--sensor-pols must provide exactly two labels, got {raw!r}")
    return parts  # type: ignore[return-value]


def _derive_obs_params(telstate_endpoint: str, capture_block_id: str) -> dict:
    telstate = katsdptelstate.TelescopeState(telstate_endpoint).view(capture_block_id)
    obs_params = telstate["obs_params"]
    if not isinstance(obs_params, dict):
        raise TypeError(f"Expected obs_params to be a dict, got {type(obs_params)!r}")
    return obs_params


def _open_capture_telstate(telstate_endpoint: str, capture_block_id: str):
    return katsdptelstate.TelescopeState(telstate_endpoint).view(capture_block_id)


def _metadata_payload(
    *,
    args: argparse.Namespace,
    obs_params: dict | None,
    experiment: str | None,
    input_vdif_dir: Path,
    final_vdif_dir: Path,
    status: str,
    antab_file: str | None = None,
    catalogue_file: str | None = None,
    catalogue_sha256: str | None = None,
    catalogue_source: str | None = None,
    rxg_file: str | None = None,
) -> dict:
    payload = {
        "capture_block_id": args.capture_block_id,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "experiment": experiment,
        "final_vdif_dir": str(final_vdif_dir),
        "input_vdif_dir": str(input_vdif_dir),
        "mode": args.mode,
        "obs_params": obs_params,
        "power_source": "telstate_mean_power" if args.mode == "antab" else None,
        "product_type": "antab" if args.mode == "antab" else "vlbimeta_metadata",
        "station_code": args.station_code,
        "status": status,
        "stream_name": args.stream_name,
        "task_name": args.name,
        "telstate_endpoint": args.telstate,
    }
    if antab_file is not None:
        payload["antab_file"] = antab_file
    if catalogue_file is not None:
        payload["catalogue_file"] = catalogue_file
    if catalogue_sha256 is not None:
        payload["catalogue_sha256"] = catalogue_sha256
    if catalogue_source is not None:
        payload["catalogue_source"] = catalogue_source
    if rxg_file is not None:
        payload["rxg_file"] = rxg_file
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("vlbimeta")

    data_dir = args.data_dir.resolve()
    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory does not exist: {data_dir}")
    if not data_dir.is_dir():
        raise NotADirectoryError(f"Expected directory for data_dir: {data_dir}")

    catalogue_dir = (args.catalogue_dir or default_catalogue_dir()).resolve()
    metadata_asset_dir = (args.metadata_dir or default_metadata_dir()).resolve()
    sensor_pols = _parse_sensor_pols(args.sensor_pols)
    paths = antab_product_paths(data_dir, args.capture_block_id)

    if paths.final_dir.exists():
        log.info("ANTAB product already exists, leaving in place: %s", paths.final_dir)
        return 0

    prepare_writing_dir(paths)
    log.info("vlbimeta entrypoint invoked")
    log.info(
        "task_name=%s capture_block_id=%s stream_name=%s mode=%s",
        args.name,
        args.capture_block_id,
        args.stream_name,
        args.mode,
    )
    log.info("catalogue_dir=%s", catalogue_dir)
    log.info("metadata_asset_dir=%s", metadata_asset_dir)
    log.info("capture_root=%s", paths.capture_root)
    log.info("vdif_dir=%s", paths.vdif_dir)
    log.info("antab_writing_dir=%s", paths.writing_dir)
    if not catalogue_dir.exists():
        log.warning("Catalogue directory not found yet: %s", catalogue_dir)
    if not metadata_asset_dir.exists():
        log.warning("Metadata asset directory not found yet: %s", metadata_asset_dir)
    if paths.vdif_dir.name.endswith(".writing"):
        log.warning("Using in-progress VDIF directory because no completed directory exists yet: %s", paths.vdif_dir)

    capture_telstate = _open_capture_telstate(args.telstate, args.capture_block_id) if args.telstate else None
    obs_params = _derive_obs_params(args.telstate, args.capture_block_id) if args.telstate else None

    if args.mode == "disabled":
        log.info("vlbimeta mode is disabled; exiting without producing a product")
        return 0

    if args.mode == "pass_through":
        experiment = derive_experiment_name(obs_params, args.experiment) if (obs_params or args.experiment) else None
        final_vdif_dir = finalise_vdif_dir(paths)
        vdif_metadata = build_vdif_product_metadata(
            capture_block_id=args.capture_block_id,
            stream_name=args.stream_name,
            obs_params=obs_params,
            final_vdif_dir=final_vdif_dir,
        )
        write_metadata_json(final_vdif_dir / "metadata.json", vdif_metadata)
        metadata_payload = _metadata_payload(
            args=args,
            obs_params=obs_params,
            experiment=experiment,
            input_vdif_dir=paths.vdif_dir,
            final_vdif_dir=final_vdif_dir,
            status="pass_through",
        )
        write_metadata_json(paths.writing_dir / "metadata.json", metadata_payload)
        finalise_product_dir(paths)
        log.info("Pass-through product completed: %s", paths.final_dir)
        return 0

    if not args.telstate:
        raise ValueError("vlbimeta requires --telstate for antab mode")

    if obs_params is None:
        raise RuntimeError("obs_params could not be loaded from telstate")

    experiment = derive_experiment_name(obs_params, args.experiment)
    log.info("experiment=%s station_code=%s", experiment, args.station_code)
    catalogue_path = None
    catalogue_info: dict[str, str] = {}
    if capture_telstate is not None:
        catalogue_path, catalogue_info = materialise_catalogue_from_telstate(
            capture_telstate,
            paths.writing_dir / "catalogue",
            fallback_experiment=experiment,
        )
    if catalogue_path is None:
        catalogue_path = resolve_catalogue_path(catalogue_dir, experiment)
        catalogue_info = {
            "catalogue_file": catalogue_path.name,
            "catalogue_source": "packaged",
        }
        log.warning("Using packaged VLBI catalogue fallback for experiment %s: %s", experiment, catalogue_path)
    rxg_path = args.rxg if args.rxg.is_absolute() else metadata_asset_dir / args.rxg

    antab_path = generate_antab_from_capture(
        experiment=experiment,
        capture_block_id=args.capture_block_id,
        stream_name=args.stream_name,
        dataset_stream_name=args.dataset_stream_name,
        telstate_endpoint=args.telstate,
        station_code=args.station_code,
        rxg_path=rxg_path,
        catalogue_path=catalogue_path,
        power_dir=paths.tsys_dir,
        metadata_dir=paths.writing_dir,
        rdb_dir=paths.writing_dir / "rdb",
        gain_tab=args.gain_tab,
        sensor_pols=sensor_pols,
        time_buffer=args.time_buffer,
    )

    final_vdif_dir = finalise_vdif_dir(paths)
    vdif_metadata = build_vdif_product_metadata(
        capture_block_id=args.capture_block_id,
        stream_name=args.stream_name,
        obs_params=obs_params,
        final_vdif_dir=final_vdif_dir,
    )
    write_metadata_json(final_vdif_dir / "metadata.json", vdif_metadata)
    metadata_payload = _metadata_payload(
        args=args,
        obs_params=obs_params,
        experiment=experiment,
        input_vdif_dir=paths.vdif_dir,
        final_vdif_dir=final_vdif_dir,
        status="completed",
        antab_file=antab_path.name,
        catalogue_file=catalogue_info.get("catalogue_file", catalogue_path.name),
        catalogue_sha256=catalogue_info.get("catalogue_sha256"),
        catalogue_source=catalogue_info.get("catalogue_source"),
        rxg_file=rxg_path.name,
    )
    write_metadata_json(paths.writing_dir / "metadata.json", metadata_payload)
    finalise_product_dir(paths)
    log.info("ANTAB product completed: %s", paths.final_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
