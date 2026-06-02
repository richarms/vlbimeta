#!/usr/bin/env python3

"""Inspect live telstate mean-power keys for a VLBI run."""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
from collections.abc import Sequence
from pathlib import Path

import aiokatcp
import katsdptelstate

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from vlbimeta.runtime import candidate_mean_power_sensor_key_sets


DEFAULT_CHANNEL_ORDER = ("lsb-pol0", "lsb-pol1", "usb-pol0", "usb-pol1")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve a product telstate endpoint and inspect actual versus expected "
            "mean-power keys for the active capture block."
        )
    )
    parser.add_argument(
        "--master-controller",
        default="lab-mc.sdp.kat.ac.za:5001",
        help="Master-controller host:port used to resolve telstate endpoint [%(default)s].",
    )
    parser.add_argument(
        "--product",
        help="Subarray product name. Required unless --telstate-endpoint is given.",
    )
    parser.add_argument(
        "--telstate-endpoint",
        help="Explicit telstate endpoint. Skips KATCP endpoint resolution if supplied.",
    )
    parser.add_argument(
        "--cbid",
        help="Explicit capture block ID. Defaults to the latest CBID discovered from *_obs_params keys.",
    )
    parser.add_argument(
        "--root-filter",
        default="*mean-power*",
        help="Glob filter used when listing root telstate keys [%(default)s].",
    )
    parser.add_argument(
        "--view-filter",
        default="*mean-power*",
        help="Glob filter used when listing capture-block-view keys [%(default)s].",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=3,
        help="Maximum number of samples to print per key [%(default)s].",
    )
    return parser.parse_args()


async def resolve_telstate_endpoint(master_controller: str, product: str) -> str:
    host, port_s = master_controller.rsplit(":", 1)
    client = aiokatcp.Client(host, int(port_s))
    try:
        await client.wait_connected()
        reply, _ = await client.request("telstate-endpoint", product)
    finally:
        client.close()
        await client.wait_closed()
    return reply[0].decode()


def discover_latest_cbid(telstate: katsdptelstate.TelescopeState) -> str:
    cbids = set()
    for key in telstate.keys("*_obs_params"):
        match = re.match(r"^(\d+)_obs_params$", key)
        if match:
            cbids.add(match.group(1))
    if not cbids:
        raise RuntimeError("Could not discover any CBIDs from '*_obs_params' keys")
    return sorted(cbids)[-1]


def short_samples(values: Sequence[tuple[object, float]], limit: int) -> list[tuple[object, float]]:
    if limit <= 0:
        return []
    return list(values[-limit:])


def print_key_report(
    telstate: katsdptelstate.TelescopeState, keys: Sequence[str], sample_limit: int, heading: str
) -> None:
    print(heading)
    if not keys:
        print("  <none>")
        return
    for key in sorted(keys):
        try:
            values = telstate.get_range(key, st=0)
        except Exception as exc:  # pragma: no cover - diagnostic path
            print(f"  {key}: ERROR {exc}")
            continue
        print(f"  {key}: {len(values)} samples")
        for value, timestamp in short_samples(values, sample_limit):
            print(f"    value={value!r} ts={timestamp}")


def main() -> None:
    args = parse_args()
    if args.telstate_endpoint is None and args.product is None:
        raise SystemExit("Either --telstate-endpoint or --product must be supplied")

    if args.telstate_endpoint is None:
        args.telstate_endpoint = asyncio.run(
            resolve_telstate_endpoint(args.master_controller, args.product)
        )

    root = katsdptelstate.TelescopeState(args.telstate_endpoint)
    cbid = args.cbid or discover_latest_cbid(root)
    view = root.view(cbid)

    print(f"telstate_endpoint: {args.telstate_endpoint}")
    print(f"cbid: {cbid}")
    if args.product:
        print(f"product: {args.product}")

    root_keys = root.keys(args.root_filter)
    view_keys = view.keys(args.view_filter)
    print_key_report(root, root_keys, args.sample_limit, f"root keys matching {args.root_filter!r}:")
    print_key_report(view, view_keys, args.sample_limit, f"view keys matching {args.view_filter!r}:")

    print("candidate key sets:")
    for key_set in candidate_mean_power_sensor_key_sets("sdp_vdif", DEFAULT_CHANNEL_ORDER):
        statuses = []
        for key in key_set:
            exists = key in view
            samples = len(view.get_range(key, st=0)) if exists else 0
            statuses.append(f"{key}={'present' if exists else 'absent'}:{samples}")
        print(f"  {'; '.join(statuses)}")


if __name__ == "__main__":
    main()
