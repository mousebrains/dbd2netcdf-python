#
# 2csv subcommand — equivalent to C++ dbd2csv
#
# Reads DBD files and outputs CSV.
#
# Feb-2026, Pat Welch, pat@mousebrains.com

from __future__ import annotations

import logging
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import Any

import numpy as np

from xarray_dbd._dbd_cpp import read_dbd_file, scan_sensors
from xarray_dbd.backend import _sort_by_header_time
from xarray_dbd.cli import logger
from xarray_dbd.cli.dbd2nc import read_sensor_list


def _format_column(col: np.ndarray, size: int) -> list[str]:
    """Format a sensor column for CSV output; sentinel/NaN → empty cell."""
    if size == 1:
        strs = col.astype(str).tolist()
        for i in np.where(col == -127)[0]:
            strs[i] = ""
        return strs
    if size == 2:
        strs = col.astype(str).tolist()
        for i in np.where(col == -32768)[0]:
            strs[i] = ""
        return strs
    # float32 (size=4) or float64 (size=8)
    fmt = "%.7g" if size == 4 else "%.15g"
    strs = [fmt % v for v in col]
    for i in np.where(~np.isfinite(col))[0]:
        strs[i] = ""
    return strs


def _add_common_args(parser) -> None:
    """Add arguments shared between the subcommand and standalone entry point."""
    parser.add_argument("files", nargs="+", type=Path, help="DBD files to process")
    parser.add_argument(
        "-c",
        "--sensors",
        type=Path,
        metavar="filename",
        help="File containing sensors to select on (criteria)",
    )
    parser.add_argument(
        "-C",
        "--cache",
        type=Path,
        metavar="directory",
        help="Directory to cache sensor list in",
    )
    parser.add_argument(
        "-k",
        "--sensor-output",
        type=Path,
        metavar="filename",
        help="File containing sensors to output",
    )
    parser.add_argument(
        "-m",
        "--skip-mission",
        action="append",
        metavar="mission",
        help="Mission to skip (can be repeated)",
    )
    parser.add_argument(
        "-M",
        "--keep-mission",
        action="append",
        metavar="mission",
        help="Mission to keep (can be repeated)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        metavar="filename",
        help="Where to store the CSV (default: stdout)",
    )
    skip_grp = parser.add_mutually_exclusive_group()
    skip_grp.add_argument(
        "-s",
        "--skip-first",
        action="store_true",
        help="Explicitly skip the first record of every file (this is the default)",
    )
    skip_grp.add_argument(
        "--keep-first",
        action="store_true",
        help="Keep the first record of every file (default is to skip)",
    )
    parser.add_argument(
        "-r",
        "--repair",
        action="store_true",
        help="Attempt to repair bad data records",
    )
    parser.add_argument(
        "--sort",
        choices=("lexicographic", "header_time", "none"),
        default="header_time",
        help="File sort order (default: header_time)",
    )
    logger.add_args(parser)


def add_args(subparsers) -> None:
    """Register the '2csv' subcommand."""
    parser = subparsers.add_parser(
        "2csv",
        help="Convert DBD files to CSV",
        description="Read Slocum glider DBD files and output CSV",
    )
    _add_common_args(parser)
    parser.set_defaults(func=run)


def run(args) -> int:
    """Execute the 2csv subcommand."""
    logger.mk_logger(args)

    for f in args.files:
        if not f.exists():
            logging.error("File not found: %s", f)
            return 1

    criteria = None
    if args.sensors:
        if not args.sensors.exists():
            logging.error("Sensors file not found: %s", args.sensors)
            return 1
        criteria = read_sensor_list(args.sensors)

    to_keep = None
    if args.sensor_output:
        if not args.sensor_output.exists():
            logging.error("Sensor output file not found: %s", args.sensor_output)
            return 1
        to_keep = read_sensor_list(args.sensor_output)

    cache_dir = args.cache
    if cache_dir is None and len(args.files) > 0:
        cache_dir = args.files[0].parent / "cache"
    if cache_dir and not cache_dir.exists():
        logging.warning("Cache directory not found: %s", cache_dir)
        cache_dir = None

    file_list = [str(f) for f in args.files]
    if args.sort == "lexicographic":
        file_list.sort()
    elif args.sort == "header_time":
        file_list = _sort_by_header_time(file_list)
    # sort == "none": preserve caller's order

    cache_str = str(cache_dir) if cache_dir else ""

    # Pass 1: discover union sensor list and valid files
    try:
        sensor_result = scan_sensors(
            file_list,
            cache_dir=cache_str,
            skip_missions=args.skip_mission or [],
            keep_missions=args.keep_mission or [],
        )
    except (OSError, RuntimeError, ValueError) as e:
        logging.error("Error scanning sensors: %s", e)
        return 1

    sensor_names = list(sensor_result["sensor_names"])
    sensor_sizes = list(sensor_result["sensor_sizes"])
    valid_files = list(sensor_result["valid_files"])

    if to_keep:
        keep_set = set(to_keep)
        indices = [i for i, n in enumerate(sensor_names) if n in keep_set]
        sensor_names = [sensor_names[i] for i in indices]
        sensor_sizes = [sensor_sizes[i] for i in indices]

    if not sensor_names:
        logging.warning("No sensors found")
        return 0

    valid_set = set(valid_files)

    fp: Any
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        fp = open(args.output, "w", encoding="utf-8")  # noqa: SIM115
    else:
        fp = sys.stdout

    try:
        # Write header
        fp.write(",".join(sensor_names) + "\n")

        n_records = 0
        file_count = 0

        for fn in file_list:
            if fn not in valid_set:
                continue
            try:
                result = read_dbd_file(
                    fn,
                    cache_dir=cache_str,
                    to_keep=to_keep or [],
                    criteria=criteria or [],
                    skip_first_record=not args.keep_first,
                    repair=args.repair,
                )
            except (OSError, RuntimeError, ValueError) as e:
                logging.warning("Skipping %s: %s", fn, e)
                continue

            n = int(result["n_records"])
            if n == 0:
                file_count += 1
                continue

            file_col_map = dict(zip(result["sensor_names"], result["columns"], strict=True))

            # Format each union column as a list of strings (empty for missing
            # sensors and for fill-value / non-finite cells).
            formatted: list[list[str]] = []
            for si, name in enumerate(sensor_names):
                col = file_col_map.get(name)
                if col is None:
                    formatted.append([""] * n)
                else:
                    formatted.append(_format_column(col, sensor_sizes[si]))

            # Write one line per record.
            write = fp.write
            for row in zip(*formatted, strict=True):
                write(",".join(row))
                write("\n")

            n_records += n
            file_count += 1

    finally:
        if fp is not sys.stdout:
            fp.close()

    logging.info("Wrote %d records, %d variables", n_records, len(sensor_names))
    return 0


def main():
    """Standalone entry point."""
    parser = ArgumentParser(
        description="Convert Slocum glider DBD files to CSV",
    )
    _add_common_args(parser)
    args = parser.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
