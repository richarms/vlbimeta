#!/usr/bin/env python3
"""Prepare a pass-through overlay of a fresh CAM config; never deploy it."""

import argparse
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
from urllib.parse import urlsplit


IMAGES = {"katsdpcontroller", "katsdpvlbi", "katsdpcam2telstate", "vlbimeta"}


def prepare(config, images, stream):
    if set(images) != IMAGES or any(
        not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", image) for image in images.values()
    ):
        raise ValueError("Supply exactly four digest-pinned smoke images")
    if config.get("version") not in ("4.8", "4.9"):
        raise ValueError("This smoke profile supports configuration versions 4.8 and 4.9")
    inputs = config["inputs"]
    output = config["outputs"][stream]
    if output["type"] != "sdp.vdif" or len(output["src_streams"]) != 1:
        raise ValueError("Select an sdp.vdif output with one resampled-voltage input")
    source = inputs[output["src_streams"][0]]
    endpoint = urlsplit(source["url"])
    if (source["type"] != "cbf.tied_array_resampled_voltage" or endpoint.scheme != "vdif"
            or not endpoint.hostname or endpoint.port is None):
        raise ValueError("Resampled voltage requires a vdif://host:port endpoint")
    parents = source["src_streams"]
    if len(set(parents)) != 2 or any(
        inputs[parent]["type"] != "cbf.tied_array_channelised_voltage" for parent in parents
    ):
        raise ValueError("Resampled voltage requires two channelised-voltage parents")
    result = copy.deepcopy(config)
    options = result.setdefault("config", {})
    options.setdefault("image_overrides", {}).update(images)
    options.setdefault("vlbimeta", {})["mode"] = "pass_through"
    return result


def catalogue(start):
    if start.tzinfo is None or start.utcoffset() != timedelta(0):
        raise ValueError("Start time must have an explicit UTC offset (+00:00 or Z)")
    rows = ["# EXPERIMENT CAMSMOKE", "# CAL_PREFIX MCAL", "# PCC_PREFIX MPCC",
            "# PCC_VERIFICATION False"]
    for index, offset in enumerate((0, 40), start=1):
        stamp = (start + timedelta(seconds=offset)).strftime("%Y-%m-%d %H:%M:%S.%f")
        rows.append(f"Smoke, azel, 0, 80, scan No{index:04d}, {stamp}, 20")
    return "\n".join(rows) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path, help="Fresh allocated CAM product-config JSON")
    parser.add_argument("--images", type=Path, required=True,
                        help="JSON with four digest-pinned images selected for this test")
    parser.add_argument("--stream", default="sdp_vdif")
    parser.add_argument("--start", required=True, help="First scan UTC, e.g. 2026-09-24T12:00:00Z")
    parser.add_argument("--output-dir", required=True, type=Path, help="New directory (must not exist)")
    args = parser.parse_args()
    config = prepare(json.loads(args.config.read_text()), json.loads(args.images.read_text()), args.stream)
    start = datetime.fromisoformat(args.start.replace("Z", "+00:00"))
    csv = catalogue(start)
    if start <= datetime.now(timezone.utc):
        parser.error("Choose a future start time; plans are never automatically shifted")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    (args.output_dir / "product-config.json").write_text(json.dumps(config, indent=2) + "\n")
    (args.output_dir / "tracking.csv").write_text(csv)
    print(f"Prepared {args.output_dir}; review and apply through CAM. No requests sent.")


if __name__ == "__main__":
    main()
