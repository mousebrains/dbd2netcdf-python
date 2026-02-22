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
import pandas as pd

from xarray_dbd._dbd_cpp import read_dbd_file, scan_sensors
from xarray_dbd.cli import logger
from xarray_dbd.cli.dbd2nc import read_sensor_list


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
    parser.add_argument(
        "-s",
        "--skip-first",
        action="store_true",
        help="Skip first record in each file except the first",
    )
    parser.add_argument(
        "-r",
        "--repair",
        action="store_true",
        help="Attempt to repair bad data records",
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

    file_list = sorted(str(f) for f in args.files)
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

    # Fill values per dtype for NaN representation in CSV
    fill_map = {1: np.int8(-127), 2: np.int16(-32768)}

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
            if fn not in valid_files:
                continue
            try:
                result = read_dbd_file(
                    fn,
                    cache_dir=cache_str,
                    to_keep=to_keep or [],
                    criteria=criteria or [],
                    skip_first_record=(args.skip_first and file_count > 0),
                    repair=args.repair,
                )
            except (OSError, RuntimeError, ValueError) as e:
                logging.warning("Skipping %s: %s", fn, e)
                continue

            n = int(result["n_records"])
            if n == 0:
                file_count += 1
                continue

            file_names = list(result["sensor_names"])
            file_cols = list(result["columns"])
            file_col_map = dict(zip(file_names, file_cols, strict=True))

            # Build DataFrame with union columns in order
            df_data = {}
            for si, name in enumerate(sensor_names):
                col = file_col_map.get(name)
                if col is not None:
                    df_data[name] = col
                else:
                    # Sensor not in this file — fill with sentinel
                    size = sensor_sizes[si]
                    fill = fill_map.get(size)
                    if fill is not None:
                        df_data[name] = np.full(n, fill, dtype=type(fill))
                    else:
                        df_data[name] = np.full(n, np.nan)

            df = pd.DataFrame(df_data)
            # NaN → empty string in CSV output
            df.to_csv(fp, header=False, index=False, na_rep="")

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
