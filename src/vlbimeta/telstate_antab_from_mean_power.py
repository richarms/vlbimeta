#!/usr/bin/env python3

"""Create ANTAB files using pre-computed mean-power sensors from telstate."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd
from astropy.time import Time as ap_time

import katdal
import katsdptelstate

from .runtime import default_mean_power_sensor_keys
from .vdif_power_antab import (
    AntabWriter,
    StationCalibrator,
    parse_chan_params,
    parse_vlbi_cat,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate ANTAB calibration tables by reading per-thread mean-power sensors "
            "from telstate instead of recomputing power from VDIF."
        )
    )
    parser.add_argument("--experiment", required=True, help="VLBI experiment name.")
    parser.add_argument("--cbid", required=True, help="Narrowband capture block ID for the observation.")
    parser.add_argument(
        "--station-code",
        default="me",
        help="Two-character VLBI station code [%(default)s].",
    )
    parser.add_argument(
        "--rxg",
        type=Path,
        default=Path("calmel.rxg"),
        help="Path to the station RXG file [%(default)s].",
    )
    parser.add_argument(
        "--rdb",
        type=Path,
        help="Path to a telescope-state RDB dump (skips download if supplied).",
    )
    parser.add_argument(
        "--redis-url",
        help="Redis URL for telescope-state access; combined with --cbid to form the dataset path.",
    )
    parser.add_argument(
        "--catalogue-dir",
        type=Path,
        default=Path("catalogues"),
        help="Directory containing VLBI catalogue CSV files [%(default)s].",
    )
    parser.add_argument(
        "--power-dir",
        type=Path,
        default=Path("pwr_files"),
        help="Directory where Tsys CSV files will be written [%(default)s].",
    )
    parser.add_argument(
        "--metadata-dir",
        type=Path,
        default=Path("metadata"),
        help="Directory holding station metadata and where ANTAB files are written [%(default)s].",
    )
    parser.add_argument(
        "--rdb-dir",
        type=Path,
        default=Path("rdb_files"),
        help="Directory used to cache downloaded RDB files when --redis-url is not supplied [%(default)s].",
    )
    parser.add_argument(
        "--gain-tab",
        type=float,
        default=0.5,
        help="Assumed tied-array beam gain (dimensionless) used when deriving Tsys [%(default)s].",
    )
    parser.add_argument(
        "--mean-power-template",
        default="{cbid}_mean_power{thread}",
        help=(
            "Python format string used to resolve mean-power sensor keys. "
            "Available fields: {cbid}, {thread}. [%(default)s]"
        ),
    )
    parser.add_argument(
        "--alt-templates",
        type=str,
        nargs="*",
        default=("{cbid}_mean-power{thread}", "mean_power{thread}", "mean-power{thread}", "{cbid}:mean_power{thread}"),
        help="Fallback templates to try when resolving sensor keys.",
    )
    parser.add_argument(
        "--time-buffer",
        type=float,
        default=0.0,
        help="Seconds to expand each scan window by when selecting power samples [%(default)s].",
    )
    return parser.parse_args()


def resolve_telstate_source(args: argparse.Namespace) -> str | None:
    if args.rdb is not None:
        return str(args.rdb)
    if args.redis_url is not None:
        return f"{args.redis_url.rstrip('/')}/{args.cbid}"
    return None


def open_katdal_dataset(
    dataset_source: str,
    capture_block_id: str | None = None,
    stream_name: str | None = None,
) -> katdal.DataSet:
    kwargs = {}
    if capture_block_id is not None:
        kwargs["capture_block_id"] = capture_block_id
    if stream_name is not None:
        kwargs["stream_name"] = stream_name
        kwargs["chunk_store"] = None
    return katdal.open(dataset_source, **kwargs)


def open_telstate_view(telstate_endpoint: str, capture_block_id: str):
    return katsdptelstate.TelescopeState(telstate_endpoint).view(capture_block_id)


def find_sensor_key(telstate, thread: int, templates: Iterable[str], cbid: str) -> str:
    for template in templates:
        key = template.format(cbid=cbid, thread=thread)
        if key in telstate:  # type: ignore[operator]
            return key
    raise KeyError(f"Unable to locate mean-power sensor for thread {thread} using provided templates.")


def load_mean_power(
    telstate,
    thread_indices: list[int],
    cbid: str,
    primary_template: str,
    fallback_templates: Iterable[str],
) -> dict[int, list[tuple[float, float]]]:
    sensor_data: dict[int, list[tuple[float, float]]] = {}
    templates = [primary_template, *fallback_templates]
    for thread in thread_indices:
        key = find_sensor_key(telstate, thread, templates, cbid)
        values = telstate.get_range(key, st=0)  # type: ignore[attr-defined]
        if not values:
            raise RuntimeError(f"Mean-power sensor '{key}' returned no samples.")
        sensor_data[thread] = values
    return sensor_data


def load_mean_power_from_keys(telstate, sensor_keys: Sequence[str]) -> dict[int, list[tuple[float, float]]]:
    sensor_data: dict[int, list[tuple[float, float]]] = {}
    for thread, key in enumerate(sensor_keys):
        if key not in telstate:  # type: ignore[operator]
            raise KeyError(f"Unable to locate mean-power sensor '{key}' in telstate.")
        values = telstate.get_range(key, st=0)  # type: ignore[attr-defined]
        if not values:
            raise RuntimeError(f"Mean-power sensor '{key}' returned no samples.")
        sensor_data[thread] = values
    return sensor_data


def build_thread_mapping(fc_chans: dict[str, float]) -> list[str]:
    per_band = defaultdict(dict)
    for name, freq in fc_chans.items():
        band, pol = name.split("-")
        per_band[band][pol] = (name, freq)
    ordered = []
    bands_sorted = sorted(per_band.items(), key=lambda item: min(val[1] for val in item[1].values()))
    for _, pol_map in bands_sorted:
        for pol in ("pol0", "pol1"):
            if pol in pol_map:
                ordered.append(pol_map[pol][0])
    return ordered


def select_samples_for_scan(
    sensor_series: dict[int, list[tuple[float, float]]],
    thread_to_channel: dict[int, str],
    start_ts: float,
    end_ts: float,
) -> pd.DataFrame:
    samples = defaultdict(dict)
    for thread, entries in sensor_series.items():
        channel = thread_to_channel[thread]
        for value, timestamp in entries:
            if start_ts <= timestamp <= end_ts:
                samples[timestamp][channel] = value
    if not samples:
        raise RuntimeError("No mean-power samples found inside the requested time window.")
    ordered_times = sorted(samples.keys())
    df = pd.DataFrame({"time_unix": ordered_times})
    all_channels = list(thread_to_channel.values())
    for channel in all_channels:
        df[channel] = [samples[timestamp].get(channel, np.nan) for timestamp in ordered_times]
    df["time"] = ap_time(df["time_unix"], format="unix").isot
    return df


def derive_tsys_files(
    calibrator: StationCalibrator,
    sensor_series: dict[int, list[tuple[float, float]]],
    channel_order: list[str],
    scan_data: dict,
    output_dir: Path,
    time_buffer: float,
) -> dict[str, Path]:
    thread_to_channel = {thread: channel_order[idx] for idx, thread in enumerate(sorted(sensor_series))}

    tsys_outputs: dict[str, Path] = {}
    ts_sols_avail = np.array(list(calibrator.G_vlbi.keys()))

    for scan_name, metadata in scan_data.items():
        start_ts = metadata["proc_start_ts"] - time_buffer
        end_ts = metadata["proc_start_ts"] + metadata["proc_duration"] + time_buffer
        df = select_samples_for_scan(sensor_series, thread_to_channel, start_ts, end_ts)
        ts_pvals = df["time_unix"].to_numpy()
        ts_sols_prev = ts_sols_avail[ts_sols_avail <= ts_pvals[0]]
        ts_sols_curr = ts_sols_avail[np.logical_and(ts_sols_avail >= ts_pvals[0], ts_sols_avail <= ts_pvals[-1])]
        if len(ts_sols_prev) >= 1:
            ts_sol = ts_sols_prev[-1]
        elif len(ts_sols_curr) >= 1:
            ts_sol = ts_sols_curr[0]
        else:
            raise RuntimeError(f"No calibration solutions found for scan '{scan_name}'.")
        tsys_df = df.drop(columns="time_unix").copy()
        for ch_name in channel_order:
            if ch_name in tsys_df.columns:
                tsys_df[ch_name] = tsys_df[ch_name] / calibrator.G_vlbi[ts_sol][ch_name]
        ordered_cols = ["time"] + [ch for ch in channel_order if ch in tsys_df.columns]
        tsys_df = tsys_df[ordered_cols]
        fn_out = output_dir / f"{calibrator.exp_name}_{scan_name.replace(' ', '')}_tsys.csv"
        fn_out.parent.mkdir(parents=True, exist_ok=True)
        tsys_df.to_csv(fn_out, index=False)
        tsys_outputs[scan_name] = fn_out
    return tsys_outputs


def main() -> None:
    args = parse_args()
    catalogue_path = args.catalogue_dir / f"vlbi_cat_{args.experiment}.csv"
    if not catalogue_path.exists():
        raise FileNotFoundError(f"Catalogue not found: {catalogue_path}")
    rxg_path = args.rxg if args.rxg.is_absolute() else args.metadata_dir / args.rxg
    if not rxg_path.exists():
        raise FileNotFoundError(f"RXG file not found: {rxg_path}")

    vex_params, scan_data, _ = parse_vlbi_cat(catalogue_path)
    if any(value is None for value in vex_params.values()) or len(vex_params["CHANNELS"]) == 0:
        raise RuntimeError("Invalid observation catalogue header.")
    antab_chan_map = {"R1": "lsb-pol0", "L1": "lsb-pol1", "R2": "usb-pol0", "L2": "usb-pol1"}
    _, bw_chan, fc_chans, antab_chan_def = parse_chan_params(vex_params, antab_chan_map)
    apply_l2c_conversion = vex_params["POL"] == "RL"

    telstate_source = resolve_telstate_source(args)
    calibrator = StationCalibrator(
        args.cbid,
        args.experiment,
        fc_chans,
        bw_chan,
        args.rdb_dir,
        args.power_dir,
        telstate_source,
        gain_tab=args.gain_tab,
    )
    dataset_path = calibrator._resolve_dataset()
    dataset = open_katdal_dataset(dataset_path)
    telstate = dataset.source.telstate
    calibrator.compute_cal_sols(circ_pol=apply_l2c_conversion)

    channel_order = build_thread_mapping(fc_chans)
    thread_indices = list(range(len(channel_order)))
    sensor_series = load_mean_power(
        telstate,
        thread_indices,
        args.cbid,
        args.mean_power_template,
        args.alt_templates,
    )
    antab_scans = [scan_name for scan_name in scan_data.keys() if scan_name.startswith("scan No")]
    if not antab_scans:
        raise RuntimeError("No scans matching 'scan No*' found in the catalogue for ANTAB generation.")
    scan_subset = {scan_name: scan_data[scan_name] for scan_name in antab_scans}

    tsys_fns = derive_tsys_files(
        calibrator,
        sensor_series,
        channel_order,
        scan_subset,
        args.power_dir,
        args.time_buffer,
    )

    antab_writer = AntabWriter(args.station_code, rxg_path, antab_chan_def)
    scans_tgts = {scan_name: scan_data[scan_name]["target"] for scan_name in scan_subset.keys()}
    antab_writer.make_file(args.experiment, tsys_fns, antab_chan_map, scans_tgts)
    antab_path = args.metadata_dir / f"{args.experiment}{args.station_code}.antab"
    antab_writer.write_to_file(antab_path)
    dataset.close()
    print(f"ANTAB written to {antab_path}")


def generate_antab_from_capture(
    *,
    experiment: str,
    capture_block_id: str,
    stream_name: str,
    dataset_stream_name: str | None,
    telstate_endpoint: str,
    station_code: str,
    rxg_path: Path,
    catalogue_path: Path,
    power_dir: Path,
    metadata_dir: Path,
    rdb_dir: Path,
    gain_tab: float = 0.5,
    sensor_pols: Sequence[str] = ("x", "y"),
    time_buffer: float = 0.0,
) -> Path:
    if not catalogue_path.exists():
        raise FileNotFoundError(f"Catalogue not found: {catalogue_path}")
    if not rxg_path.exists():
        raise FileNotFoundError(f"RXG file not found: {rxg_path}")

    vex_params, scan_data, _ = parse_vlbi_cat(catalogue_path)
    if any(value is None for value in vex_params.values()) or len(vex_params["CHANNELS"]) == 0:
        raise RuntimeError("Invalid observation catalogue header.")
    antab_chan_map = {"R1": "lsb-pol0", "L1": "lsb-pol1", "R2": "usb-pol0", "L2": "usb-pol1"}
    _, bw_chan, fc_chans, antab_chan_def = parse_chan_params(vex_params, antab_chan_map)
    apply_l2c_conversion = vex_params["POL"] == "RL"

    dataset_source = f"redis://{telstate_endpoint}"
    telstate = open_telstate_view(telstate_endpoint, capture_block_id)
    calibrator = StationCalibrator(
        capture_block_id,
        experiment,
        fc_chans,
        bw_chan,
        rdb_dir,
        power_dir,
        dataset_source,
        gain_tab=gain_tab,
        dataset_capture_block_id=capture_block_id,
        dataset_stream_name=dataset_stream_name,
    )
    calibrator.compute_cal_sols(circ_pol=apply_l2c_conversion)

    channel_order = build_thread_mapping(fc_chans)
    sensor_keys = default_mean_power_sensor_keys(stream_name, channel_order, sensor_pols=sensor_pols)
    sensor_series = load_mean_power_from_keys(telstate, sensor_keys)
    antab_scans = [scan_name for scan_name in scan_data.keys() if scan_name.startswith("scan No")]
    if not antab_scans:
        raise RuntimeError("No scans matching 'scan No*' found in the catalogue for ANTAB generation.")
    scan_subset = {scan_name: scan_data[scan_name] for scan_name in antab_scans}

    tsys_fns = derive_tsys_files(
        calibrator,
        sensor_series,
        channel_order,
        scan_subset,
        power_dir,
        time_buffer,
    )

    antab_writer = AntabWriter(station_code, rxg_path, antab_chan_def)
    scans_tgts = {scan_name: scan_data[scan_name]["target"] for scan_name in scan_subset.keys()}
    antab_writer.make_file(experiment, tsys_fns, antab_chan_map, scans_tgts)
    antab_path = metadata_dir / f"{experiment}{station_code}.antab"
    antab_writer.write_to_file(antab_path)
    return antab_path


if __name__ == "__main__":
    main()
