#!/usr/bin/env python3
"""Resolve aliases and download/validate model payloads from this execution host.

Requires katsdpmodels[requests]. Model arguments are TYPE=URL pairs, where TYPE
is rfi, band or beam. Choose URLs from the actual product band/ratio/antennas.
"""

import argparse
from datetime import datetime, timezone
import json
import socket

import requests
from katsdpmodels.band_mask import BandMask
from katsdpmodels.fetch.requests import Fetcher
from katsdpmodels.primary_beam import PrimaryBeam
from katsdpmodels.rfi_mask import RFIMask


class TimedSession(requests.Session):
    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", (10, 30))
        return super().request(*args, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("models", nargs="+", help="e.g. rfi=https://sdpmodels.kat.ac.za/rfi_mask/current.alias")
    args = parser.parse_args()
    types = {"rfi": RFIMask, "band": BandMask, "beam": PrimaryBeam}
    entries = []
    for spec in args.models:
        kind, separator, url = spec.partition("=")
        if not separator or kind not in types or not url.startswith(("http://", "https://")):
            parser.error("Models must be rfi=URL, band=URL or beam=URL over HTTP(S)")
        entries.append((kind, url))
    report = dict(host=socket.getfqdn(), checked_at=datetime.now(timezone.utc).isoformat(), models=[])
    with TimedSession() as session, Fetcher(session=session) as fetcher:
        for kind, url in entries:
            result = dict(type=kind, url=url, ok=False)
            try:
                result["chain"] = fetcher.resolve(url)
                model = fetcher.get(url, types[kind])
                result.update(ok=True, version=model.version, checksum=model.checksum)
            except Exception as exc:
                result.update(error_type=type(exc).__name__, error=str(exc))
            report["models"].append(result)
    print(json.dumps(report, indent=2))
    return 0 if all(entry["ok"] for entry in report["models"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
