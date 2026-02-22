#
# missions subcommand — list missions and file counts from DBD file headers
#
# Feb-2026, Pat Welch, pat@mousebrains.com

from __future__ import annotations

import logging
import sys
from collections import Counter

import xarray_dbd as xdbd
from xarray_dbd.cli import logger
from xarray_dbd.cli.sensors import _add_common_args


def add_args(subparsers) -> None:
    """Register the 'missions' subcommand."""
    parser = subparsers.add_parser(
        "missions",
        help="List missions and file counts from DBD file headers",
        description="Scan DBD file headers and output mission names with file counts",
    )
    _add_common_args(parser)
    parser.set_defaults(func=run)


def run(args) -> int:
    """Execute the missions subcommand."""
    logger.mk_logger(args)

    filenames = [str(f) for f in args.files]
    for f in args.files:
        if not f.exists():
            logging.error("File not found: %s", f)
            return 1

    result = xdbd.scan_headers(
        filenames,
        skip_missions=args.skip_mission,
        keep_missions=args.keep_mission,
    )

    missions = result["mission_names"]
    if not missions:
        logging.warning("No valid files found")
        return 1

    counts = Counter(missions)
    lines = []
    for mission in sorted(counts):
        lines.append(f"{counts[mission]} {mission}")

    output = "\n".join(lines) + "\n"

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
        logging.info("Wrote mission list to %s", args.output)
    else:
        sys.stdout.write(output)

    return 0
