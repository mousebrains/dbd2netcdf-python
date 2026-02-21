#
# missions subcommand — list missions and file counts from DBD file headers
#
# Feb-2026, Pat Welch, pat@mousebrains.com

import logging
import sys
from collections import Counter
from pathlib import Path

import xarray_dbd as xdbd
from xarray_dbd.cli import logger


def addArgs(subparsers) -> None:
    """Register the 'missions' subcommand."""
    parser = subparsers.add_parser(
        "missions",
        help="List missions and file counts from DBD file headers",
        description="Scan DBD file headers and output mission names with file counts",
    )
    parser.add_argument("files", nargs="+", type=Path, help="DBD files to scan")
    parser.add_argument(
        "-C",
        "--cache",
        type=str,
        default="",
        metavar="directory",
        help="Directory to cache sensor list in",
    )
    parser.add_argument(
        "-m",
        "--skipMission",
        action="append",
        default=[],
        metavar="mission",
        help="Mission to skip (can be repeated)",
    )
    parser.add_argument(
        "-M",
        "--keepMission",
        action="append",
        default=[],
        metavar="mission",
        help="Mission to keep (can be repeated)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        metavar="filename",
        help="Where to store the output (default: stdout)",
    )
    logger.addArgs(parser)
    parser.set_defaults(func=run)


def run(args) -> int:
    """Execute the missions subcommand."""
    logger.mkLogger(args)

    filenames = [str(f) for f in args.files]
    for f in args.files:
        if not f.exists():
            logging.error("File not found: %s", f)
            return 1

    result = xdbd.scan_headers(
        filenames,
        skip_missions=args.skipMission,
        keep_missions=args.keepMission,
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
