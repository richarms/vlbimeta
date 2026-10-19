"""Validate and snapshot the canonical CSV plan without reading or copying VDIF."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Sequence

import katsdptelstate
from katsdptelstate.errors import TelstateError

from .catalogue import parse_vlbi_catalogue
from .manifest import build_scan_manifest, manifest_entries_from_catalogue, write_scan_manifest
from .runtime import MAX_CATALOGUE_BYTES, materialise_catalogue_from_telstate


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--catalogue", type=Path, help="Local CSV observation plan")
    source.add_argument("--telstate", help="Read the capture-scoped plan from host:port")
    parser.add_argument("--capture-block-id", help="Required for telstate input")
    parser.add_argument("--output-dir", type=Path, required=True, help="Fresh diagnostic output directory")
    args = parser.parse_args(argv)
    if args.telstate and not args.capture_block_id:
        parser.error("--telstate requires --capture-block-id")
    try:
        if args.catalogue:
            with args.catalogue.open("rb") as stream:
                content = stream.read(MAX_CATALOGUE_BYTES + 1)
            inputs = {
                "vlbi_catalogue_content": content,
                "vlbi_catalogue_sha256": hashlib.sha256(content).hexdigest(),
                "vlbi_catalogue_name": args.catalogue.name,
                "vlbi_catalogue_format": "csv",
            }
        else:
            inputs = katsdptelstate.TelescopeState(args.telstate)
        args.output_dir.mkdir(parents=True, exist_ok=False)
        path, info = materialise_catalogue_from_telstate(
            inputs, args.output_dir / "catalogue", capture_block_id=args.capture_block_id,
        )
        if args.catalogue:
            info["catalogue_source"] = "explicit_file"
        catalogue = parse_vlbi_catalogue(path)
        entries = manifest_entries_from_catalogue(catalogue)
        manifest = build_scan_manifest(entries, source=info["catalogue_source"] + "_catalogue")
        manifest.update(experiment=catalogue.obs_params["EXPERIMENT"], catalogue=info)
        if args.capture_block_id:
            manifest["capture_block_id"] = args.capture_block_id
        output = args.output_dir / "scan_manifest.json"
        write_scan_manifest(output, manifest)
    except (ValueError, KeyError, OSError, TelstateError) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    print(f"{output}: {len(entries)} entries, {sum(e.include for e in entries)} scheduled VLBI scans")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
