#!/usr/bin/env python3
#
# Modified version of mkOne.py to use the Python xarray-dbd implementation
# instead of the C++ dbd2netCDF
#
# Supports both directory walking (like mkTwo.py) and explicit file lists.
#
# Oct-2023, Pat Welch, pat@mousebrains.com
# Modified Jan-2025 to use xarray-dbd
# Modified Feb-2026 for directory walking and performance

from __future__ import annotations

import logging
import multiprocessing
import os
import re
import sys
import time
from argparse import ArgumentParser, Namespace
from pathlib import Path

import xarray_dbd as xdbd
from xarray_dbd.cli import logger


def process_files(
    ofn: str, filenames: list[str], args: Namespace, sensors_filename: str | None = None
) -> None:
    """Process files using xarray-dbd"""
    start_time = time.time()
    logging.info("%s: %s files", ofn, len(filenames))

    # Make sure the directory of the output file exists
    odir = os.path.dirname(ofn)
    if odir and not os.path.isdir(odir):
        logging.info("Creating %s", odir)
        os.makedirs(odir, mode=0o755, exist_ok=True)

    # Read sensor list if provided
    to_keep = None
    if sensors_filename:
        with open(sensors_filename, encoding="utf-8") as f:
            to_keep = [line.strip() for line in f if line.strip()]

    # Prepare arguments for xarray-dbd
    skip_missions = args.exclude if args.exclude else None
    keep_missions = args.include if args.include else None
    cache_dir = Path(args.cache) if args.cache else None

    # Stream directly to NetCDF without holding all data in memory
    try:
        n_records, n_files = xdbd.write_multi_dbd_netcdf(
            [Path(f) for f in filenames],
            ofn,
            skip_first_record=not args.keep_first,
            repair=args.repair,
            to_keep=to_keep,
            skip_missions=skip_missions,
            keep_missions=keep_missions,
            cache_dir=cache_dir,
            compression=5,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logging.error("Failed to process files for %s: %s", ofn, e)
        return

    if n_records == 0:
        logging.warning("No data for %s, skipping", ofn)
        return

    logging.info(
        "Wrote %s with %d records from %d files in %.2f seconds",
        ofn,
        n_records,
        n_files,
        time.time() - start_time,
    )


def extract_sensors(filenames: list[str], args: Namespace) -> list[str]:
    """Extract unique sensor names from files"""
    all_sensors = set()

    cache_dir = str(Path(args.cache)) if args.cache else ""

    for filename in filenames:
        try:
            result = xdbd.read_dbd_file(
                str(filename),
                cache_dir=cache_dir,
                skip_first_record=False,
            )
            all_sensors.update(result["sensor_names"])
        except (OSError, RuntimeError, ValueError) as e:
            logging.warning("Error reading %s: %s", filename, e)

    return list(all_sensors)


def process_all(
    filenames: list[str], args: Namespace, suffix: str, sensors_filename: str | None = None
) -> None:
    """Process files into a NetCDF"""
    filenames = list(filenames)  # ensure it is a list
    if not filenames:
        return  # Nothing to do

    filenames.sort()  # Sort for consistent processing order

    ofn = args.output_prefix + suffix  # Output filename
    process_files(ofn, filenames, args, sensors_filename)


def write_sensors(sensors: set[str], ofn: str) -> str:
    odir = os.path.dirname(ofn)
    if odir and not os.path.isdir(odir):
        logging.info("Creating %s", odir)
        os.makedirs(odir, mode=0o755, exist_ok=True)

    with open(ofn, "w", encoding="utf-8") as fp:
        fp.write("\n".join(sorted(sensors)))
        fp.write("\n")
    return ofn


def process_dbd(filenames: list[str], args: Namespace) -> None:
    """Process flight Dinkum Binary files"""
    filenames = list(filenames)
    if not filenames:
        return  # Nothing to do

    all_sensors = set(extract_sensors(filenames, args))
    dbd_sensors = {x for x in all_sensors if x.startswith(("m_", "c_"))}
    sci_sensors = {x for x in all_sensors if x.startswith("sci_")}
    other_sensors = all_sensors.difference(dbd_sensors).difference(sci_sensors)
    sci_sensors.add("m_present_time")
    other_sensors.add("m_present_time")

    write_sensors(all_sensors, args.output_prefix + "dbd.all.sensors")
    dbd_fn = write_sensors(dbd_sensors, args.output_prefix + "dbd.sensors")
    sci_fn = write_sensors(sci_sensors, args.output_prefix + "dbd.sci.sensors")
    other_fn = write_sensors(other_sensors, args.output_prefix + "dbd.other.sensors")

    process_all(filenames, args, "dbd.nc", dbd_fn)
    process_all(filenames, args, "dbd.sci.nc", sci_fn)
    process_all(filenames, args, "dbd.other.nc", other_fn)


def discover_files(paths: list[str]) -> dict[str, list[str]]:
    """Discover DBD files from paths (directories or file lists).

    For directories, walks recursively matching *.?[bc]d patterns.
    For files, categorizes them by type key.

    Returns:
        Dict mapping type key ('d', 'e', 's', 't', 'm', 'n') to sorted file lists
    """
    files: dict[str, list[str]] = {}
    # Single regex matching all DBD type keys, compiled once
    dbd_pattern = re.compile(r"\.([demnst])[bc]d$", re.IGNORECASE)

    for path in paths:
        path = os.path.abspath(os.path.expanduser(path))

        if os.path.isdir(path):
            # Walk directory tree like mkTwo.py
            for dirpath, _, filenames in os.walk(path):
                for fn in filenames:
                    m = dbd_pattern.search(fn)
                    if m:
                        key = m.group(1).lower()
                        files.setdefault(key, []).append(os.path.join(dirpath, fn))
        elif os.path.isfile(path):
            # Categorize individual file
            fn = os.path.basename(path)
            m = dbd_pattern.search(fn)
            if m:
                key = m.group(1).lower()
                files.setdefault(key, []).append(path)
        else:
            logging.warning("%s is not a file or directory, skipping", path)

    # Sort file lists for consistent processing order
    for key in files:
        files[key].sort()

    return files


def _add_common_args(parser) -> None:
    """Add arguments shared between the subcommand and standalone entry point."""
    parser.add_argument(
        "path",
        type=str,
        nargs="+",
        help="Dinkum binary files or directories to convert",
    )
    grp = parser.add_argument_group(description="Processing options")
    grp.add_argument("--cache", type=str, default="cache", help="Directory for sensor cache files")
    grp.add_argument("--repair", action="store_true", help="Should corrupted files be 'repaired'")
    grp.add_argument(
        "--keep-first",
        action="store_true",
        help="Should the first record not be discarded on all the files?",
    )
    g = grp.add_mutually_exclusive_group()
    g.add_argument("--exclude", type=str, action="append", help="Mission(s) to exclude")
    g.add_argument("--include", type=str, action="append", help="Mission(s) to include")

    grp = parser.add_argument_group(description="Output related arguments")
    grp.add_argument("--output-prefix", type=str, required=True, help="Output prefix")

    logger.add_args(parser)


def add_args(subparsers) -> None:
    """Register the 'mkone' subcommand."""
    parser = subparsers.add_parser(
        "mkone",
        help="Batch process directories of DBD files",
        description="Discover and convert directories of Slocum glider DBD files to NetCDF",
    )
    _add_common_args(parser)
    parser.set_defaults(func=run)


def _worker(ofn, filenames, args, sensors_filename=None):
    """Multiprocessing worker — sets up logging then processes one output file."""
    logger.mk_logger(args)
    process_files(ofn, filenames, args, sensors_filename)


def run(args) -> int:
    """Execute the mkone batch processing."""
    logger.mk_logger(args)

    if args.exclude is None and args.include is None:
        args.exclude = (
            "status.mi",
            "lastgasp.mi",
            "initial.mi",
            "overtime.mi",
            "ini0.mi",
            "ini1.mi",
            "ini2.mi",
            "ini3.mi",
        )

    args.cache = os.path.abspath(os.path.expanduser(args.cache))

    if not os.path.isdir(args.cache):
        logging.info("Creating %s", args.cache)
        os.makedirs(args.cache, mode=0o755, exist_ok=True)

    start_time = time.time()

    files = discover_files(args.path)

    # Collect work items: (ofn, filenames, sensors_filename)
    work: list[tuple[str, list[str], str | None]] = []

    if "d" in files:
        d_files = sorted(files["d"])

        # Sensor extraction and partitioning (fast, sequential)
        all_sensors = set(extract_sensors(d_files, args))
        dbd_sensors = {x for x in all_sensors if x.startswith(("m_", "c_"))}
        sci_sensors = {x for x in all_sensors if x.startswith("sci_")}
        other_sensors = all_sensors.difference(dbd_sensors).difference(sci_sensors)
        sci_sensors.add("m_present_time")
        other_sensors.add("m_present_time")

        write_sensors(all_sensors, args.output_prefix + "dbd.all.sensors")
        dbd_fn = write_sensors(dbd_sensors, args.output_prefix + "dbd.sensors")
        sci_fn = write_sensors(sci_sensors, args.output_prefix + "dbd.sci.sensors")
        other_fn = write_sensors(other_sensors, args.output_prefix + "dbd.other.sensors")

        work.append((args.output_prefix + "dbd.nc", d_files, dbd_fn))
        work.append((args.output_prefix + "dbd.sci.nc", d_files, sci_fn))
        work.append((args.output_prefix + "dbd.other.nc", d_files, other_fn))

    for key in ["e", "s", "t", "m", "n"]:
        if key in files:
            work.append((args.output_prefix + key + "bd.nc", sorted(files[key]), None))

    if not work:
        logging.info("No files to process")
        return 0

    # Spawn one process per output file
    processes = []
    for ofn, flist, sensors_fn in work:
        p = multiprocessing.Process(target=_worker, args=(ofn, flist, args, sensors_fn))
        processes.append((p, ofn))
        p.start()

    # Wait for all to complete
    failed = False
    for p, ofn in processes:
        p.join()
        if p.exitcode != 0:
            logging.error("Worker for %s exited with code %d", ofn, p.exitcode)
            failed = True

    logging.info("All processing complete in %.2f seconds", time.time() - start_time)
    return 1 if failed else 0


def main():
    """Standalone entry point for mkone command."""
    parser = ArgumentParser()
    _add_common_args(parser)
    args = parser.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
