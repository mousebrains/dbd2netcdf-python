#
# sensors subcommand — equivalent to C++ dbdSensors
#
# Scans DBD file headers and outputs the unified sensor list without reading data.
#
# Feb-2026, Pat Welch, pat@mousebrains.com

from __future__ import annotations

import logging
import sys
from argparse import ArgumentParser
from pathlib import Path

import xarray_dbd as xdbd
from xarray_dbd.cli import logger


def _add_common_args(parser) -> None:
    """Add arguments shared between the subcommand and standalone entry point."""
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
        "--skip-mission",
        action="append",
        default=[],
        metavar="mission",
        help="Mission to skip (can be repeated)",
    )
    parser.add_argument(
        "-M",
        "--keep-mission",
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
    logger.add_args(parser)


def add_args(subparsers) -> None:
    """Register the 'sensors' subcommand."""
    parser = subparsers.add_parser(
        "sensors",
        help="List sensors from DBD file headers",
        description="Scan DBD file headers and output the unified sensor list (no data read)",
    )
    _add_common_args(parser)
    parser.set_defaults(func=run)


def run(args) -> int:
    """Execute the sensors subcommand."""
    logger.mk_logger(args)

    filenames = [str(f) for f in args.files]
    for f in args.files:
        if not f.exists():
            logging.error("File not found: %s", f)
            return 1

    cache_dir = args.cache
    result = xdbd.scan_sensors(
        filenames,
        cache_dir=cache_dir,
        skip_missions=args.skip_mission,
        keep_missions=args.keep_mission,
    )

    if result["n_files"] == 0:
        logging.warning("No valid files found")
        return 1

    names = result["sensor_names"]
    units = result["sensor_units"]
    sizes = result["sensor_sizes"]

    lines = []
    for size, name, unit in zip(sizes, names, units, strict=True):
        lines.append(f"{size} {name} {unit}")

    output = "\n".join(lines) + "\n"

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
        logging.info("Wrote sensor list to %s", args.output)
    else:
        sys.stdout.write(output)

    return 0


def main():
    """Standalone entry point."""
    parser = ArgumentParser(
        description="Scan DBD file headers and output the unified sensor list",
    )
    _add_common_args(parser)
    args = parser.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
