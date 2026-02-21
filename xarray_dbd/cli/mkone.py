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

import logging
import os
import re
import sys
import time
from argparse import ArgumentParser
from pathlib import Path

import xarray_dbd as xdbd


def processFiles(
    ofn: str, filenames: list, args: ArgumentParser, sensorsFilename: str = None
) -> None:
    """Process files using xarray-dbd"""
    stime = time.time()
    logging.info("%s: %s files", ofn, len(filenames))

    # Make sure the directory of the output file exists
    odir = os.path.dirname(ofn)
    if odir and not os.path.isdir(odir):
        logging.info("Creating %s", odir)
        os.makedirs(odir, mode=0o755, exist_ok=True)

    # Read sensor list if provided
    to_keep = None
    if sensorsFilename:
        with open(sensorsFilename) as f:
            to_keep = [line.strip() for line in f if line.strip()]

    # Prepare arguments for xarray-dbd
    skip_missions = args.exclude if args.exclude else None
    keep_missions = args.include if args.include else None
    cache_dir = Path(args.cache) if args.cache else None

    # Read the files
    try:
        ds = xdbd.open_multi_dbd_dataset(
            [Path(f) for f in filenames],
            skip_first_record=not args.keepFirst,
            repair=args.repair,
            to_keep=to_keep,
            skip_missions=skip_missions,
            keep_missions=keep_missions,
            cache_dir=cache_dir,
        )
    except (OSError, ValueError, RuntimeError) as e:
        logging.error("Failed to read files for %s: %s", ofn, e)
        return

    if len(ds.data_vars) == 0:
        logging.warning("No data variables for %s, skipping", ofn)
        return

    # Write to NetCDF with compression to match C++ dbd2netCDF output
    encoding = {
        var: {"zlib": True, "complevel": 5, "chunksizes": (min(5000, len(ds.i)),)}
        for var in ds.data_vars
    }
    ds.to_netcdf(ofn, encoding=encoding)
    logging.info(
        "Wrote %s with %d records and %d variables in %.2f seconds",
        ofn, len(ds.i), len(ds.data_vars), time.time() - stime,
    )


def extractSensors(filenames: list, args: ArgumentParser) -> list:
    """Extract unique sensor names from files"""
    all_sensors = set()

    cache_dir = str(Path(args.cache)) if args.cache else ""

    for filename in filenames:
        try:
            result = xdbd.read_dbd_file(
                str(filename), cache_dir=cache_dir, skip_first_record=False,
            )
            all_sensors.update(result["sensor_names"])
        except Exception as e:
            logging.warning("Error reading %s: %s", filename, e)

    return list(all_sensors)


def processAll(
    filenames: list, args: ArgumentParser, suffix: str, sensorsFilename: str = None
) -> None:
    """Process files into a NetCDF"""
    filenames = list(filenames)  # ensure it is a list
    if not filenames:
        return  # Nothing to do

    filenames.sort()  # Sort for consistent processing order

    ofn = args.outputPrefix + suffix  # Output filename
    processFiles(ofn, filenames, args, sensorsFilename)


def writeSensors(sensors: set, ofn: str) -> None:
    odir = os.path.dirname(ofn)
    if odir and not os.path.isdir(odir):
        logging.info("Creating %s", odir)
        os.makedirs(odir, mode=0o755, exist_ok=True)

    with open(ofn, "w") as fp:
        fp.write("\n".join(sorted(sensors)))
        fp.write("\n")
    return ofn


def processDBD(filenames: list, args: ArgumentParser) -> None:
    """Process flight Dinkum Binary files"""
    filenames = list(filenames)
    if not filenames:
        return  # Nothing to do

    allSensors = set(extractSensors(filenames, args))
    dbdSensors = set(filter(lambda x: x.startswith("m_") or x.startswith("c_"), allSensors))
    sciSensors = set(filter(lambda x: x.startswith("sci_"), allSensors))
    otroSensors = allSensors.difference(dbdSensors).difference(sciSensors)
    sciSensors.add("m_present_time")
    otroSensors.add("m_present_time")

    writeSensors(allSensors, args.outputPrefix + "dbd.all.sensors")
    dbdFN = writeSensors(dbdSensors, args.outputPrefix + "dbd.sensors")
    sciFN = writeSensors(sciSensors, args.outputPrefix + "dbd.sci.sensors")
    otroFN = writeSensors(otroSensors, args.outputPrefix + "dbd.other.sensors")

    processAll(filenames, args, "dbd.nc", dbdFN)
    processAll(filenames, args, "dbd.sci.nc", sciFN)
    processAll(filenames, args, "dbd.other.nc", otroFN)


def discover_files(paths: list[str]) -> dict[str, list[str]]:
    """Discover DBD files from paths (directories or file lists).

    For directories, walks recursively matching *.?[bc]d patterns.
    For files, categorizes them by type key.

    Returns:
        Dict mapping type key ('d', 'e', 's', 't', 'm', 'n') to sorted file lists
    """
    files: dict[str, list[str]] = {}

    for path in paths:
        path = os.path.abspath(os.path.expanduser(path))

        if os.path.isdir(path):
            # Walk directory tree like mkTwo.py
            for dirpath, _, filenames in os.walk(path):
                for fn in filenames:
                    for key in ["d", "e", "m", "n", "s", "t"]:
                        if re.search(r"[.]" + key + r"[bc]d$", fn, re.IGNORECASE):
                            files.setdefault(key, []).append(os.path.join(dirpath, fn))
                            break
        elif os.path.isfile(path):
            # Categorize individual file
            fn = os.path.basename(path)
            for key in ["d", "e", "m", "n", "s", "t"]:
                if re.search(r"[.]" + key + r"[bc]d$", fn, re.IGNORECASE):
                    files.setdefault(key, []).append(path)
                    break
        else:
            logging.warning("%s is not a file or directory, skipping", path)

    # Sort file lists for consistent processing order
    for key in files:
        files[key].sort()

    return files


def main():
    """Main entry point for mkone command"""
    parser = ArgumentParser()
    parser.add_argument(
        "path", type=str, nargs="+",
        help="Dinkum binary files or directories to convert",
    )

    grp = parser.add_argument_group(description="Processing options")
    grp.add_argument("--cache", type=str, default="cache", help="Directory for sensor cache files")
    grp.add_argument("--verbose", action="store_true", help="Verbose output")
    grp.add_argument("--repair", action="store_true", help="Should corrupted files be 'repaired'")
    grp.add_argument(
        "--keepFirst",
        action="store_true",
        help="Should the first record not be discarded on all the files?",
    )
    g = grp.add_mutually_exclusive_group()
    g.add_argument("--exclude", type=str, action="append", help="Mission(s) to exclude")
    g.add_argument("--include", type=str, action="append", help="Mission(s) to include")

    grp = parser.add_argument_group(description="Output related arguments")
    grp.add_argument("--outputPrefix", type=str, required=True, help="Output prefix")

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s %(levelname)s: %(message)s",
        level=logging.DEBUG if args.verbose else logging.INFO,
    )

    if args.exclude is None and args.include is None:
        args.exclude = (  # Default missions to exclude
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

    stime = time.time()

    # Discover files from paths (directories or explicit files)
    files = discover_files(args.path)

    # Process each type sequentially to avoid memory exhaustion.
    # Each type already uses ProcessPoolExecutor for parallelism internally.
    if "d" in files:
        processDBD(files["d"], args)

    for key in ["e", "s", "t", "m", "n"]:
        if key in files:
            processAll(files[key], args, key + "bd.nc")

    logging.info("All processing complete in %.2f seconds", time.time() - stime)
    return 0


if __name__ == "__main__":
    sys.exit(main())
