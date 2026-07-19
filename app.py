#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from opend_copytrader.server import run


def main() -> None:
    parser = argparse.ArgumentParser(description="Moonvest local app")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8899)
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--ready-file", help=argparse.SUPPRESS)
    args = parser.parse_args()
    run(
        args.host,
        args.port,
        open_browser=not args.no_browser,
        ready_file=Path(args.ready_file) if args.ready_file else None,
    )


if __name__ == "__main__":
    main()
