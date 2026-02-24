"""Controller-facing `vlbimeta` entrypoint.

MVP contract for katsdpcontroller post-processing:
- Positional args: ``<data_dir> <capture_block_id> <stream_name>``
- Exit status ``0``: arguments accepted and task completed (currently a no-op)
- Non-zero exit: invalid invocation or inaccessible required paths

This command intentionally performs no post-processing yet. It exists to
stabilise the CLI contract and container wiring before the analysis pipeline is
implemented.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Sequence

from .paths import default_catalogue_dir, default_metadata_dir


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="vlbimeta.py",
        description="katsdpcontroller VLBI post-processing entrypoint (MVP no-op).",
    )
    parser.add_argument("data_dir", type=Path, help="Shared capture volume mounted by katsdpcontroller.")
    parser.add_argument("capture_block_id", help="Capture block ID for the completed observation.")
    parser.add_argument("stream_name", help="VDIF stream name for this post-processing task.")
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
    metadata_dir = (args.metadata_dir or default_metadata_dir()).resolve()

    log.info("vlbimeta entrypoint invoked (MVP no-op)")
    log.info("capture_block_id=%s stream_name=%s", args.capture_block_id, args.stream_name)
    log.info("data_dir=%s", data_dir)
    log.info("catalogue_dir=%s", catalogue_dir)
    log.info("metadata_dir=%s", metadata_dir)
    if not catalogue_dir.exists():
        log.warning("Catalogue directory not found yet: %s", catalogue_dir)
    if not metadata_dir.exists():
        log.warning("Metadata directory not found yet: %s", metadata_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
