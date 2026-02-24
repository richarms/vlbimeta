#!/usr/bin/env python3

"""Compute per-thread power statistics from shrapnel VDIF directories and save a CSV/plot."""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from pathlib import Path
from typing import Sequence

import astropy.units as u
import baseband.vdif
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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
        description="Generate a power CSV and plot from a directory of shrapnel VDIF files belonging to a single observation."
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
        help="Aggregate samples into this duration when computing average power [%(default)s].",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="Output CSV filepath [<vdif_stem>_pwr.csv].",
    )
    parser.add_argument(
        "--plot",
        type=Path,
        help="Output plot filepath [<vdif_stem>_power.png].",
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
            raise ValueError(
                f"Not enough power samples ({len(df)}) to trim {trim} from each end."
            )
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


def main() -> None:
    args = parse_args()
    vdif_files = _list_vdif_files(args.vdif_dir)
    csv_out, plot_out = _resolve_outputs(vdif_files[0], args.csv, args.plot)
    _check_collision([csv_out, plot_out], args.overwrite)

    power_df = compute_power_table(vdif_files, args.chunk_seconds, args.labels, args.trim)
    power_df.to_csv(csv_out, index=False)
    save_plot(power_df, plot_out)


if __name__ == "__main__":
    main()
