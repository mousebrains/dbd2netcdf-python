#!/usr/bin/env python3
"""
Python implementation of dbd2netCDF
Converts Slocum glider DBD files to NetCDF format
"""

import argparse
import sys
import tempfile
from pathlib import Path

import xarray as xr

import xarray_dbd as xdbd


def read_sensor_list(filename: Path) -> list[str]:
    """Read sensor names from a file (one per line or comma/space separated)"""
    sensors = []
    with open(filename, encoding="utf-8") as f:
        for line in f:
            # Remove comments
            line = line.split("#")[0].strip()
            if not line:
                continue
            # Handle comma or space separated
            parts = line.replace(",", " ").split()
            sensors.extend(parts)
    return sensors


def main():
    parser = argparse.ArgumentParser(
        description="Convert Slocum glider DBD files to NetCDF format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Report bugs to pat@mousebrains.com",
    )

    parser.add_argument("files", nargs="+", type=Path, help="DBD files to process")
    parser.add_argument("-a", "--append", action="store_true", help="Append to the NetCDF file")
    parser.add_argument(
        "-c",
        "--sensors",
        type=Path,
        metavar="filename",
        help="file containing sensors to select on (criteria)",
    )
    parser.add_argument(
        "-C", "--cache", type=Path, metavar="directory", help="directory to cache sensor list in"
    )
    parser.add_argument(
        "-k",
        "--sensorOutput",
        type=Path,
        metavar="filename",
        help="file containing sensors to output",
    )
    parser.add_argument(
        "-m",
        "--skipMission",
        action="append",
        metavar="mission",
        help="mission to skip (can be repeated)",
    )
    parser.add_argument(
        "-M",
        "--keepMission",
        action="append",
        metavar="mission",
        help="mission to keep (can be repeated)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        metavar="filename",
        required=True,
        help="where to store the data",
    )
    parser.add_argument(
        "-s",
        "--skipFirst",
        action="store_true",
        help="Skip first record in each file except the first",
    )
    parser.add_argument(
        "-r", "--repair", action="store_true", help="attempt to repair bad data records"
    )
    parser.add_argument("-V", "--version", action="version", version=f"%(prog)s {xdbd.__version__}")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="enable some diagnostic output"
    )

    args = parser.parse_args()

    # Validate inputs
    for f in args.files:
        if not f.exists():
            print(f"Error: File not found: {f}", file=sys.stderr)
            return 1

    # Read sensor lists if provided
    criteria = None
    if args.sensors:
        if not args.sensors.exists():
            print(f"Error: Sensors file not found: {args.sensors}", file=sys.stderr)
            return 1
        criteria = read_sensor_list(args.sensors)
        if args.verbose:
            print(f"Loaded {len(criteria)} criteria sensors from {args.sensors}")

    to_keep = None
    if args.sensorOutput:
        if not args.sensorOutput.exists():
            print(f"Error: Sensor output file not found: {args.sensorOutput}", file=sys.stderr)
            return 1
        to_keep = read_sensor_list(args.sensorOutput)
        if args.verbose:
            print(f"Loaded {len(to_keep)} output sensors from {args.sensorOutput}")

    # Determine cache directory
    cache_dir = args.cache
    if cache_dir is None and len(args.files) > 0:
        # Default to cache subdirectory next to first file
        cache_dir = args.files[0].parent / "cache"

    if cache_dir and not cache_dir.exists():
        if args.verbose:
            print(f"Warning: Cache directory not found: {cache_dir}")
        cache_dir = None

    # Check if output exists and we're not appending
    if args.output.exists() and not args.append:
        # Overwrite existing file (like dbd2netCDF does)
        if args.verbose:
            print(f"Overwriting existing file: {args.output}")

    try:
        # Read the DBD files
        if args.verbose:
            print(f"Processing {len(args.files)} file(s)...")

        # Use multi-file reader
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

        if args.verbose:
            print(f"Read {len(ds.i)} records")
            print(f"Output {len(ds.data_vars)} variables")

        # Write to NetCDF
        if args.append and args.output.exists():
            # Append mode: read existing, concatenate, write to temp then rename
            if args.verbose:
                print(f"Appending to {args.output}")
            try:
                with xr.open_dataset(args.output) as ds_existing:
                    ds_combined = xr.concat([ds_existing, ds], dim="i")
                tmp_fd, tmp_path = tempfile.mkstemp(suffix=".nc", dir=args.output.parent)
                try:
                    import os

                    os.close(tmp_fd)
                    ds_combined.to_netcdf(tmp_path)
                    Path(tmp_path).replace(args.output)
                except Exception:
                    Path(tmp_path).unlink(missing_ok=True)
                    raise
            except (OSError, ValueError) as e:
                print(f"Error appending to {args.output}: {e}", file=sys.stderr)
                return 1
        else:
            if args.verbose:
                print(f"Writing to {args.output}")
            ds.to_netcdf(args.output)

        if args.verbose:
            print(f"Successfully wrote {args.output}")

        return 0

    except (OSError, ValueError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
