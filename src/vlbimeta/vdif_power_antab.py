#!/usr/bin/env python3

"""Read and plot shrapnel VDIF power levels and generate ANTAB calibration products."""

from __future__ import annotations

import argparse
import urllib.request
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

import astropy.units as u
import baseband.vdif
import katdal
import katpoint
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import scipy.ndimage
from astropy.time import Time as ap_time, TimeDelta


def _parse_labels(raw: str | None) -> list[str] | None:
    if raw is None:
        return None
    labels = [label.strip() for label in raw.split(",") if label.strip()]
    if not labels:
        raise argparse.ArgumentTypeError("at least one non-empty label is required")
    return labels


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute power summaries from shrapnel VDIF files and optionally create ANTAB outputs."
    )
    parser.add_argument(
        "vdif_dir",
        type=Path,
        help="Directory containing shrapnel VDIF files (processed in lexicographic order).",
    )
    parser.add_argument(
        "--chunk-seconds",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="Aggregate samples over this duration when computing average power [%(default)s].",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="Output CSV filepath for the combined power table [<vdif_stem>_pwr.csv].",
    )
    parser.add_argument(
        "--plot",
        type=Path,
        help="Output plot filepath for the combined power plot [<vdif_stem>_power.png].",
    )
    parser.add_argument(
        "--labels",
        type=_parse_labels,
        help="Comma-separated labels for VDIF threads (defaults to thread0,thread1,...).",
    )
    parser.add_argument(
        "--trim",
        type=int,
        default=3,
        metavar="COUNT",
        help="Number of leading and trailing power samples to discard to avoid edge effects [%(default)s].",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting existing output files.",
    )
    parser.add_argument(
        "--experiment",
        help="VLBI experiment name (required for ANTAB generation).",
    )
    parser.add_argument(
        "--cbid",
        help="Narrowband capture block ID for the observation (required for ANTAB generation).",
    )
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
        help="Directory containing resampler power CSV files (and where Tsys CSVs will be written) [%(default)s].",
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
        help="Directory used to cache downloaded RDB files [%(default)s].",
    )
    parser.add_argument(
        "--gain-tab",
        type=float,
        default=0.5,
        help="Assumed tied-array beam gain (dimensionless) used when deriving Tsys [%(default)s].",
    )
    return parser.parse_args()


def _resolve_outputs(reference_path: Path, csv_path: Path | None, plot_path: Path | None) -> tuple[Path, Path]:
    stem = reference_path.stem
    csv_out = csv_path or reference_path.with_name(f"{stem}_pwr.csv")
    plot_out = plot_path or reference_path.with_name(f"{stem}_power.png")
    return csv_out, plot_out


def _check_collision(paths: Sequence[Path], overwrite: bool) -> None:
    if overwrite:
        return
    collisions = [path for path in paths if path.exists()]
    if collisions:
        joined = ", ".join(str(path) for path in collisions)
        raise FileExistsError(f"Output file(s) already exist: {joined}. Use --overwrite to replace them.")


def _thread_labels(stream, explicit: list[str] | None) -> list[str]:
    n_threads = stream.shape[1]
    if explicit is not None:
        if len(explicit) != n_threads:
            raise ValueError(f"Expected {n_threads} labels, received {len(explicit)}.")
        return list(explicit)
    return [f"thread{i}" for i in range(n_threads)]


def _list_vdif_files(directory: Path) -> list[Path]:
    if not directory.exists():
        raise FileNotFoundError(f"VDIF directory not found: {directory}")
    if not directory.is_dir():
        raise NotADirectoryError(f"Expected a directory containing VDIF files: {directory}")
    files = sorted(p for p in directory.iterdir() if p.suffix.lower() == ".vdif")
    if not files:
        raise FileNotFoundError(f"No .vdif files found in directory {directory}")
    return files


def _shard_counts(start: int, length: int, shard_count: int) -> list[int]:
    base = length // shard_count
    remainder = length % shard_count
    counts = [base] * shard_count
    offset = start % shard_count
    for i in range(remainder):
        counts[(offset + i) % shard_count] += 1
    return counts


def compute_power_table(
    vdif_paths: Sequence[Path], chunk_seconds: float, requested_labels: list[str] | None, trim: int
) -> pd.DataFrame:
    if chunk_seconds <= 0:
        raise ValueError("--chunk-seconds must be > 0.")
    if trim < 0:
        raise ValueError("--trim must be >= 0.")

    with ExitStack() as stack:
        streams = [stack.enter_context(baseband.vdif.open(path, "rs")) for path in vdif_paths]

        sample_rate = streams[0].sample_rate.to_value(u.Hz)
        if sample_rate <= 0:
            raise RuntimeError("VDIF sample rate is not positive.")

        samples_per_chunk = int(round(chunk_seconds * sample_rate))
        samples_per_chunk = max(samples_per_chunk, 1)

        labels = _thread_labels(streams[0], requested_labels)
        start_time = streams[0].start_time

        n_threads = streams[0].shape[1]
        n_shards = len(streams)

        for stream in streams[1:]:
            if not np.isclose(stream.sample_rate.to_value(u.Hz), sample_rate, rtol=0, atol=1e-6):
                raise RuntimeError("Sample rate mismatch between shrapnel files.")
            if stream.shape[1] != n_threads:
                raise RuntimeError("Thread count mismatch between shrapnel files.")

        total_samples_per_shard = [int(stream.shape[0]) for stream in streams]
        total_samples = sum(total_samples_per_shard)
        consumed = [0] * n_shards

        records: list[dict[str, float | str]] = []
        chunk_start = 0

        while chunk_start < total_samples:
            target = min(samples_per_chunk, total_samples - chunk_start)
            shard_counts = _shard_counts(chunk_start, target, n_shards)

            sum_sq = np.zeros(n_threads, dtype=np.float64)
            samples_accumulated = 0

            for idx, stream in enumerate(streams):
                required = shard_counts[idx]
                remaining = total_samples_per_shard[idx] - consumed[idx]
                take = min(required, remaining)
                if take <= 0:
                    continue
                chunk = stream.read(take)
                if chunk is None or chunk.size == 0:
                    continue
                actual = int(chunk.shape[0])
                consumed[idx] += actual
                samples_accumulated += actual
                chunk_arr = chunk.astype(np.float64, copy=False)
                sum_sq += np.sum(chunk_arr * chunk_arr, axis=0)

            if samples_accumulated == 0:
                break

            power_vals = sum_sq / samples_accumulated
            time_offset = TimeDelta(chunk_start / sample_rate, format="sec")
            record: dict[str, float | str] = {"time": (start_time + time_offset).isot}
            for label, value in zip(labels, power_vals):
                record[label] = value
            records.append(record)

            chunk_start += samples_accumulated

    if not records:
        raise RuntimeError("No samples were read from the VDIF files.")

    df = pd.DataFrame.from_records(records, columns=["time"] + labels)
    if trim:
        if len(df) <= 2 * trim:
            raise ValueError(f"Not enough power samples ({len(df)}) to trim {trim} from each end.")
        df = df.iloc[trim:-trim].reset_index(drop=True)
    return df


def save_plot(df: pd.DataFrame, plot_path: Path) -> None:
    plt.switch_backend("Agg")
    fig, ax = plt.subplots(figsize=(10, 4))
    time_axis = pd.to_datetime(df["time"])
    for column in df.columns[1:]:
        ax.plot(time_axis, df[column], marker=".", label=column)
    ax.set_title("Per-thread power")
    ax.set_ylabel("Power (counts^2)")
    ax.set_xlabel("UTC Time")
    ax.grid(True)
    ax.legend(loc="upper right")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(plot_path)
    plt.close(fig)


def parse_vlbi_cat(vlbi_cat_fn: Path, proc_buffer_sec: int = 1, ref_ant=None):
    with open(vlbi_cat_fn, "r") as cat_file:
        csv_lines = [line for line in cat_file.readlines()]
    header_lines = [line[1:].strip() for line in csv_lines if line.startswith("#")]
    hdr_keys, hdr_ch_key = ["EXPERIMENT", "POL", "CAL_PREFIX"], "CH"
    obs_params = dict.fromkeys(hdr_keys)
    obs_params["CHANNELS"] = {}
    for line in header_lines:
        hdr_key, hdr_par = line.split(" ")[0], line.split(" ")[1:]
        if hdr_key in hdr_keys:
            obs_params[hdr_key] = " ".join(hdr_par)
        elif line.startswith(hdr_ch_key):
            obs_params["CHANNELS"][hdr_key] = hdr_par
    scan_params = {}
    scan_lines = [line for line in csv_lines if not line.startswith("#")]
    for scan_line in scan_lines:
        scan_name = scan_line.split(",")[-3].strip()
        start_time_str = scan_line.split(",")[-2].strip()
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S.%f")
        start_time = start_time.replace(tzinfo=timezone.utc)
        start_time_proc = start_time - timedelta(seconds=proc_buffer_sec)
        duration = int(scan_line.split(",")[-1])
        tgt_string = scan_line.split(",")[0].split("|")[0].strip()
        tgt_string = tgt_string[1:] if tgt_string.startswith("*") else tgt_string
        kp_target_ln = ", ".join([kp.strip(" ") for kp in scan_line.split(",")[:-3]])
        scan_params[scan_name] = {
            "target": tgt_string,
            "start_iso": scan_line.split(",")[-2],
            "start_ts": start_time.timestamp(),
            "duration": duration,
            "kp_tgt": katpoint.Target(kp_target_ln, antenna=ref_ant),
            "proc_start_iso": start_time_proc.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "proc_start_ts": start_time_proc.timestamp(),
            "proc_duration": duration + 2 * proc_buffer_sec,
        }
    sorted_params = sorted(scan_params.items(), key=lambda e: e[1]["start_ts"])
    scan_params = dict(sorted_params)
    kp_target_list = []
    for _, scan_pars in scan_params.items():
        if scan_pars["kp_tgt"] not in kp_target_list:
            kp_target_list.append(scan_pars["kp_tgt"])
    vlbi_cat = katpoint.Catalogue(kp_target_list, antenna=ref_ant)
    return obs_params, scan_params, vlbi_cat


def parse_chan_params(vex_params: dict, chan_map: dict[str, str]):
    fc_chans, chan_def = {}, {}
    for vex_ch_desc in vex_params["CHANNELS"].values():
        fc_band_MHz, sb_chan, bw_chan_MHz, pol_chan = vex_ch_desc
        fc_band_MHz, bw_chan_MHz = float(fc_band_MHz), float(bw_chan_MHz)
        fc_chan_MHz = fc_band_MHz - bw_chan_MHz / 2.0 if sb_chan == "LSB" else fc_band_MHz + bw_chan_MHz / 2.0
        tsys_pol_name = "pol0" if pol_chan == "RCP" else "pol1"
        tsys_ch_name = sb_chan.lower() + "-" + tsys_pol_name
        fc_chans[tsys_ch_name] = fc_chan_MHz * 1e6
        chan_def[tsys_ch_name] = [fc_chan_MHz, sb_chan, bw_chan_MHz]
    antab_chan_def = {antab_ch: chan_def[chan_map[antab_ch]] for antab_ch in chan_map.keys()}
    return fc_band_MHz * 1e6, bw_chan_MHz * 1e6, fc_chans, antab_chan_def


class AntabWriter:
    def __init__(self, station_name: str, path_rxg: Path, chan_def: dict[str, list], print_rxg: bool = False):
        self.station_name = station_name.upper()
        self.rxg_pars = self.read_rxg_parameters(path_rxg, print_rxg)
        self.chan_def = chan_def
        self.file_lines: list[str] = []

    def read_rxg_parameters(self, path_rxg: Path, print_output: bool) -> dict:
        rxg_pars: dict[str, list | str] = {}
        with open(path_rxg, "r") as rxg_file:
            rxg_lines = rxg_file.readlines()
            rxg_lines = [line.strip() for line in rxg_lines if not line.startswith("*")]
            rxg_pars["LO"] = rxg_lines[0]
            rxg_pars["DATE"] = rxg_lines[1]
            rxg_pars["FWHM"] = rxg_lines[2]
            rxg_pars["POLS"] = rxg_lines[3]
            rxg_pars["DPFU"] = rxg_lines[4]
            rxg_pars["GAIN"] = rxg_lines[5]
            tcal_lines = []
            ii_trec = None
            for ii_l in range(6, len(rxg_lines)):
                if rxg_lines[ii_l] == "end_tcal_table":
                    ii_trec = ii_l + 1
                    break
                tcal_lines.append(rxg_lines[ii_l])
            rxg_pars["TCAL"] = tcal_lines
            if ii_trec is None:
                raise RuntimeError("RXG file is missing 'end_tcal_table'.")
            rxg_pars["TREC"] = rxg_lines[ii_trec]
            spillover_lines = []
            for ii_l in range(ii_trec + 1, len(rxg_lines)):
                if rxg_lines[ii_l] == "end_spillover_table":
                    break
                spillover_lines.append(rxg_lines[ii_l])
            rxg_pars["SPILL"] = spillover_lines
        if print_output:
            print("RXG parameters:")
            for key, par in rxg_pars.items():
                print(f"{key}: {par}")
        return rxg_pars

    def add_lines(self, lines: list[str]) -> None:
        self.file_lines.extend(lines)

    def header_lines(self, exp_name: str) -> list[str]:
        date_today = datetime.now().strftime("%Y-%m-%d")
        return [
            f"! Amplitude calibration data for {self.station_name} in {exp_name}.",
            f"! Produced on {date_today}.",
        ]

    def gain_lines(self) -> list[str]:
        gain_line = f"GAIN {self.station_name} ELEV DPFU="
        gain_line += ",".join(self.rxg_pars["DPFU"].split())
        chan_fcs = [ch_data[0] for ch_data in self.chan_def.values()]
        chan_bw = [ch_data[-1] for ch_data in self.chan_def.values()][0]
        min_freq, max_freq = min(chan_fcs) - chan_bw, max(chan_fcs) + chan_bw
        gain_line += f" FREQ={min_freq:.2f},{max_freq:.2f}"
        gain_line += " POLY="
        gain_line += ",".join(self.rxg_pars["GAIN"].split()[2:])
        gain_line += " /"
        return [gain_line]

    def make_file(self, exp_name: str, tsys_fns: dict[str, Path], channel_map: dict[str, str], tgt_scans: dict[str, str]):
        self.add_lines(self.header_lines(exp_name))
        self.add_lines(self.gain_lines())
        self.add_lines(["/"])
        self.add_lines([f"TSYS {self.station_name} FT = 1.0 TIMEOFF=0"])
        chans_ordered = sorted([ch for ch in self.chan_def.keys() if ch.startswith("R")])
        chans_ordered += sorted([ch for ch in self.chan_def.keys() if ch.startswith("L")])
        index_line = ",".join([f"'{ch}'" for ch in chans_ordered])
        self.add_lines([f"INDEX= {index_line}"])
        self.add_lines(["/"])
        for ii_ch, ch in enumerate(chans_ordered):
            ch_line = (
                f"!Column {ii_ch + 1} = {ch}: {self.chan_def[ch][0]} MHz, {self.chan_def[ch][1]}, "
                f"BW={self.chan_def[ch][2]} MHz"
            )
            self.add_lines([ch_line])
        for scan_name, tsys_fn in tsys_fns.items():
            df = pd.read_csv(tsys_fn)
            t_axis = ap_time(list(df["time"].array), format="isot")
            p_vals = {ch: df[channel_map[ch]].array for ch in chans_ordered}
            t_val = t_axis[0].to_value("yday")
            _, day, hh, mm, ss = t_val.split(":")
            minute_float = int(mm) + float(ss) / 60.0
            scan_num = scan_name.split(" ")[-1]
            sline = f"! {day} {hh}:{minute_float:05.2f}: "
            sline += f"scanNum={scan_num[-4:]} scanName={scan_num} source={tgt_scans[scan_name]}"
            self.add_lines([sline])
            for idx, t_point in enumerate(t_axis):
                t_val = t_point.to_value("yday")
                _, day, hh, mm, ss = t_val.split(":")
                minute_float = int(mm) + float(ss) / 60.0
                pline = f"{day} {hh}:{minute_float:05.2f}"
                for ch in chans_ordered:
                    pline += f" {p_vals[ch][idx]:.1f}"
                self.add_lines([pline])
        self.add_lines(["/"])

    def write_to_file(self, fn_antab: Path) -> None:
        fn_antab.parent.mkdir(parents=True, exist_ok=True)
        with open(fn_antab, "w") as antab_file:
            for line in self.file_lines:
                antab_file.write(line)
                antab_file.write("\n")


class StationCalibrator:
    def __init__(
        self,
        obs_cbid: str,
        exp_name: str,
        fc_chans: dict[str, float],
        bw_chan: float,
        rdb_dir: Path,
        out_dir: Path,
        telstate_source: str | None,
        gain_tab: float = 0.5,
    ):
        self.obs_cbid = obs_cbid
        self.exp_name = exp_name
        self.fc_chans = fc_chans
        self.bw_chan = bw_chan
        self.rdb_dir = Path(rdb_dir)
        self.out_dir = Path(out_dir)
        self.telstate_source = telstate_source
        self.gain_tab = gain_tab

    def _download_rdb(self, destination: Path) -> None:
        url = f"http://archive-gw-1.kat.ac.za/{self.obs_cbid}/{self.obs_cbid}_sdp_l0.full.rdb"
        destination.parent.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(url, destination)

    def _resolve_dataset(self) -> str:
        if self.telstate_source:
            path_candidate = Path(self.telstate_source)
            if path_candidate.exists():
                return str(path_candidate)
            return self.telstate_source
        default_path = self.rdb_dir / f"{self.obs_cbid}_sdp_l0.full.rdb"
        if not default_path.exists():
            self._download_rdb(default_path)
        return str(default_path)

    def clean_bandpass(self, bp_gains: dict[str, np.ndarray], cal_channel_freqs: np.ndarray, max_gap_Hz: float):
        clean_gains: dict[str, np.ndarray] = {}
        for inp, bp in bp_gains.items():
            flagged = np.isnan(bp)
            if flagged.all():
                clean_gains[inp] = np.zeros_like(bp)
                continue
            chans = np.arange(len(bp))
            interp_bp = np.interp(chans, chans[~flagged], bp[~flagged])
            gaps, n_gaps = scipy.ndimage.label(flagged)
            for n in range(n_gaps):
                gap = np.nonzero(gaps == n + 1)[0]
                gap_freqs = cal_channel_freqs[gap]
                lower = gap_freqs.min()
                upper = gap_freqs.max()
                if upper - lower > max_gap_Hz:
                    interp_bp[gap] = np.nan
            clean_gains[inp] = interp_bp
        return clean_gains

    def compute_cal_sols(self, clean_bandpass: bool = True, clean_maxgap_hz: float = 50.0e6, circ_pol: bool = False):
        dataset = self._resolve_dataset()
        d = katdal.open(dataset)
        ant_names = [ant.name for ant in d.ants]
        pols = d.source.telstate["cal_pol_ordering"]
        freqs = d.freqs
        g_sols_list = d.source.telstate.get_range(self.obs_cbid + "_cal_product_G", st=0)
        b_sols_list0 = d.source.telstate.get_range(self.obs_cbid + "_cal_product_B0", st=0)
        b_sols_list1 = d.source.telstate.get_range(self.obs_cbid + "_cal_product_B1", st=0)
        b_sols_list2 = d.source.telstate.get_range(self.obs_cbid + "_cal_product_B2", st=0)
        b_sols_list3 = d.source.telstate.get_range(self.obs_cbid + "_cal_product_B3", st=0)
        ts_sols = [g_sols_list[ii][1] for ii in range(len(g_sols_list))]
        g_sols_dict = {cal_res[1]: cal_res[0] for cal_res in g_sols_list}
        b_sols_dict = {}
        for ii_ts in range(len(ts_sols)):
            b_sols_st = np.vstack(
                [
                    b_sols_list0[ii_ts][0],
                    b_sols_list1[ii_ts][0],
                    b_sols_list2[ii_ts][0],
                    b_sols_list3[ii_ts][0],
                ]
            )
            b_sols_dict[ts_sols[ii_ts]] = b_sols_st
        gb_sols: dict[float, dict[str, np.ndarray]] = {}
        for ts in ts_sols:
            gb_sols[ts] = {}
            for ant in ant_names:
                for pol in pols:
                    g_sols = g_sols_dict[ts][pols.index(pol), ant_names.index(ant)]
                    bp_sols = b_sols_dict[ts][:, pols.index(pol), ant_names.index(ant)]
                    gb_sols[ts][ant + pol] = g_sols * bp_sols
        if clean_bandpass:
            for ts in ts_sols:
                gb_sols[ts] = self.clean_bandpass(gb_sols[ts], freqs, clean_maxgap_hz)
        self.G_rec_cpj = {}
        for ts in ts_sols:
            self.G_rec_cpj[ts] = {
                ant + pol: np.abs(gb_sols[ts][ant + pol]) ** 2 for ant in ant_names for pol in pols
            }
        k_tab = self.gain_tab / np.sqrt(len(d.ants))
        self.G_tab_cpj = {}
        for ts in ts_sols:
            coh_sum_v = np.nansum(np.squeeze([gb_sols[ts][ant + "v"] for ant in ant_names]), axis=0)
            coh_sum_h = np.nansum(np.squeeze([gb_sols[ts][ant + "h"] for ant in ant_names]), axis=0)
            if circ_pol:
                coh_sum_pol0 = (coh_sum_v + 1j * coh_sum_h) / np.sqrt(2)
                coh_sum_pol1 = (coh_sum_v - 1j * coh_sum_h) / np.sqrt(2)
            else:
                coh_sum_pol0 = coh_sum_v
                coh_sum_pol1 = coh_sum_h
            self.G_tab_cpj[ts] = {
                "pol0": 4 * k_tab**2 * np.abs(coh_sum_pol0) ** 2,
                "pol1": 4 * k_tab**2 * np.abs(coh_sum_pol1) ** 2,
            }
        self.G_vlbi = {}
        G_rs = 1.0
        for ts in ts_sols:
            self.G_vlbi[ts] = {}
            for name_ch, fc_ch in self.fc_chans.items():
                f_min, f_max = fc_ch - self.bw_chan / 2.0, fc_ch + self.bw_chan / 2.0
                sel_freqs = np.logical_and(freqs >= f_min, freqs <= f_max)
                name_pol = name_ch.split("-")[-1]
                self.G_vlbi[ts][name_ch] = G_rs * np.nanmean(self.G_tab_cpj[ts][name_pol][sel_freqs])

    def write_tsys_files(self, pwr_fns: dict[str, Path]) -> dict[str, Path]:
        tsys_outputs: dict[str, Path] = {}
        for scan_name, pwr_fn in pwr_fns.items():
            pwr_df = pd.read_csv(pwr_fn)
            hdr_keys = pwr_df.keys().to_list()
            t_axis = ap_time(list(pwr_df["time"].array), format="isot")
            ts_pvals = t_axis.to_value("unix")
            ts_sols_avail = np.array(list(self.G_vlbi.keys()))
            ts_sols_prev = ts_sols_avail[ts_sols_avail <= ts_pvals[0]]
            ts_sols_curr = ts_sols_avail[
                np.logical_and(ts_sols_avail >= ts_pvals[0], ts_sols_avail <= ts_pvals[-1])
            ]
            if len(ts_sols_prev) >= 1:
                ts_sol = ts_sols_prev[-1]
            elif len(ts_sols_curr) >= 1:
                ts_sol = ts_sols_curr[0]
            else:
                raise RuntimeError("No calibration solutions found within allowed time range.")
            tsys_df = pwr_df.copy()
            for ch_name in hdr_keys[1:]:
                tsys_df[ch_name] /= self.G_vlbi[ts_sol][ch_name]
            fn_out = self.out_dir / f"{self.exp_name}_{scan_name.replace(' ', '')}_tsys.csv"
            fn_out.parent.mkdir(parents=True, exist_ok=True)
            tsys_df.to_csv(fn_out, index=False)
            tsys_outputs[scan_name] = fn_out
        return tsys_outputs


def generate_antab(args: argparse.Namespace, telstate_source: str | None) -> Path | None:
    if not args.experiment or not args.cbid:
        return None

    catalogue_dir = args.catalogue_dir
    power_dir = args.power_dir
    metadata_dir = args.metadata_dir
    rdb_dir = args.rdb_dir
    rxg_path = args.rxg if args.rxg.is_absolute() else metadata_dir / args.rxg
    catalogue_path = catalogue_dir / f"vlbi_cat_{args.experiment}.csv"

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

    antab_scans = [scan_name for scan_name in scan_data.keys() if scan_name.startswith("scan No")]
    pwr_fns = {
        scan_name: power_dir / f"{args.experiment}_{scan_name.replace(' ', '')}_pwr.csv" for scan_name in antab_scans
    }
    missing = [path for path in pwr_fns.values() if not path.exists()]
    if missing:
        missing_str = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"Missing power CSV files required for Tsys conversion: {missing_str}")

    calibrator = StationCalibrator(
        args.cbid,
        args.experiment,
        fc_chans,
        bw_chan,
        rdb_dir,
        power_dir,
        telstate_source,
        gain_tab=args.gain_tab,
    )
    calibrator.compute_cal_sols(circ_pol=apply_l2c_conversion)
    tsys_fns = calibrator.write_tsys_files(pwr_fns)

    antab_writer = AntabWriter(args.station_code, rxg_path, antab_chan_def)
    scans_tgts = {scan_name: scan_data[scan_name]["target"] for scan_name in antab_scans}
    antab_writer.make_file(args.experiment, tsys_fns, antab_chan_map, scans_tgts)
    antab_path = metadata_dir / f"{args.experiment}{args.station_code}.antab"
    antab_writer.write_to_file(antab_path)
    return antab_path


def main() -> None:
    args = parse_args()
    vdif_files = _list_vdif_files(args.vdif_dir)
    csv_out, plot_out = _resolve_outputs(vdif_files[0], args.csv, args.plot)
    _check_collision([csv_out, plot_out], args.overwrite)

    power_df = compute_power_table(vdif_files, args.chunk_seconds, args.labels, args.trim)
    power_df.to_csv(csv_out, index=False)
    save_plot(power_df, plot_out)

    telstate_source: str | None = None
    if args.rdb is not None:
        telstate_source = str(args.rdb)
    elif args.redis_url is not None and args.cbid:
        telstate_source = f"{args.redis_url.rstrip('/')}/{args.cbid}"

    if args.experiment and args.cbid:
        antab_path = generate_antab(args, telstate_source)
        if antab_path is not None:
            print(f"ANTAB written to {antab_path}")


if __name__ == "__main__":
    main()
