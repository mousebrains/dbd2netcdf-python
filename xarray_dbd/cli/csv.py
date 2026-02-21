#
# 2csv subcommand — equivalent to C++ dbd2csv
#
# Reads DBD files and outputs CSV.
#
# Feb-2026, Pat Welch, pat@mousebrains.com

import logging
import sys
from argparse import ArgumentParser
from pathlib import Path

import numpy as np

import xarray_dbd as xdbd
from xarray_dbd.cli import logger
from xarray_dbd.cli.dbd2nc import read_sensor_list


def addArgs(subparsers) -> None:
    """Register the '2csv' subcommand."""
    parser = subparsers.add_parser(
        "2csv",
        help="Convert DBD files to CSV",
        description="Read Slocum glider DBD files and output CSV",
    )
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
        "--sensorOutput",
        type=Path,
        metavar="filename",
        help="File containing sensors to output",
    )
    parser.add_argument(
        "-m",
        "--skipMission",
        action="append",
        metavar="mission",
        help="Mission to skip (can be repeated)",
    )
    parser.add_argument(
        "-M",
        "--keepMission",
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
    parser.add_argument(
        "-s",
        "--skipFirst",
        action="store_true",
        help="Skip first record in each file except the first",
    )
    parser.add_argument(
        "-r",
        "--repair",
        action="store_true",
        help="Attempt to repair bad data records",
    )
    logger.addArgs(parser)
    parser.set_defaults(func=run)


def run(args) -> int:
    """Execute the 2csv subcommand."""
    logger.mkLogger(args)

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
    if args.sensorOutput:
        if not args.sensorOutput.exists():
            logging.error("Sensor output file not found: %s", args.sensorOutput)
            return 1
        to_keep = read_sensor_list(args.sensorOutput)

    cache_dir = args.cache
    if cache_dir is None and len(args.files) > 0:
        cache_dir = args.files[0].parent / "cache"
    if cache_dir and not cache_dir.exists():
        logging.warning("Cache directory not found: %s", cache_dir)
        cache_dir = None

    try:
        ds = xdbd.open_multi_dbd_dataset(
            args.files,
            skip_first_record=args.skipFirst,
            repair=args.repair,
            to_keep=to_keep,
            criteria=criteria,
            skip_missions=args.skipMission,
            keep_missions=args.keepMission,
            cache_dir=cache_dir,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logging.error("Error reading files: %s", e)
        return 1

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        fp = open(args.output, "w", encoding="utf-8")
    else:
        fp = sys.stdout

    try:
        var_names = list(ds.data_vars)
        fp.write(",".join(var_names) + "\n")

        n_records = len(ds.i)
        for r in range(n_records):
            vals = []
            for name in var_names:
                v = ds[name].values[r]
                if isinstance(v, (float, np.floating)) and np.isnan(v):
                    vals.append("")
                else:
                    vals.append(str(v))
            fp.write(",".join(vals) + "\n")
    finally:
        if fp is not sys.stdout:
            fp.close()

    logging.info("Wrote %d records, %d variables", n_records, len(var_names))
    return 0


def main():
    """Standalone entry point."""
    parser = ArgumentParser(
        description="Convert Slocum glider DBD files to CSV",
    )
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
        "--sensorOutput",
        type=Path,
        metavar="filename",
        help="File containing sensors to output",
    )
    parser.add_argument(
        "-m",
        "--skipMission",
        action="append",
        metavar="mission",
        help="Mission to skip (can be repeated)",
    )
    parser.add_argument(
        "-M",
        "--keepMission",
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
    parser.add_argument(
        "-s",
        "--skipFirst",
        action="store_true",
        help="Skip first record in each file except the first",
    )
    parser.add_argument(
        "-r",
        "--repair",
        action="store_true",
        help="Attempt to repair bad data records",
    )
    logger.addArgs(parser)
    args = parser.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
