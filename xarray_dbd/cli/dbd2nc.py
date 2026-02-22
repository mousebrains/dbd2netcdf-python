#!/usr/bin/env python3
"""
Convert Slocum glider DBD files to NetCDF format.
"""

from __future__ import annotations

import logging
import sys
import tempfile
from argparse import ArgumentParser
from pathlib import Path

import xarray as xr

import xarray_dbd as xdbd
from xarray_dbd.cli import logger


def read_sensor_list(filename: Path) -> list[str]:
    """Read sensor names from a file (one per line or comma/space separated)"""
    sensors = []
    with open(filename, encoding="utf-8") as f:
        for line in f:
            line = line.split("#")[0].strip()
            if not line:
                continue
            parts = line.replace(",", " ").split()
            sensors.extend(parts)
    return sensors


def _add_common_args(parser) -> None:
    """Add arguments shared between the subcommand and standalone entry point."""
    parser.add_argument("files", nargs="+", type=Path, help="DBD files to process")
    parser.add_argument("-a", "--append", action="store_true", help="Append to the NetCDF file")
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
        required=True,
        help="Where to store the data",
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
    parser.add_argument(
        "--compression",
        type=int,
        default=5,
        metavar="level",
        help="NetCDF compression level 1-9 (default: 5, <=0 to disable)",
    )
    logger.addArgs(parser)


def addArgs(subparsers) -> None:
    """Register the '2nc' subcommand."""
    parser = subparsers.add_parser(
        "2nc",
        help="Convert DBD files to NetCDF",
        description="Convert Slocum glider DBD files to NetCDF format",
    )
    _add_common_args(parser)
    parser.set_defaults(func=run)


def _nc_encoding(ds, complevel: int) -> dict | None:
    """Build NetCDF encoding dict with zlib compression, or None if disabled."""
    if complevel <= 0:
        return None
    return {
        var: {"zlib": True, "complevel": complevel, "chunksizes": (min(5000, len(ds.i)),)}
        for var in ds.data_vars
    }


def run(args) -> int:
    """Execute the 2nc / dbd2nc conversion."""
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
        logging.info("Loaded %d criteria sensors from %s", len(criteria), args.sensors)

    to_keep = None
    if args.sensorOutput:
        if not args.sensorOutput.exists():
            logging.error("Sensor output file not found: %s", args.sensorOutput)
            return 1
        to_keep = read_sensor_list(args.sensorOutput)
        logging.info("Loaded %d output sensors from %s", len(to_keep), args.sensorOutput)

    cache_dir = args.cache
    if cache_dir is None and len(args.files) > 0:
        cache_dir = args.files[0].parent / "cache"
    if cache_dir and not cache_dir.exists():
        logging.warning("Cache directory not found: %s", cache_dir)
        cache_dir = None

    if args.output.exists() and not args.append:
        logging.info("Overwriting existing file: %s", args.output)

    try:
        logging.info("Processing %d file(s)...", len(args.files))

        if args.append and args.output.exists():
            # Append mode: load everything into memory to concatenate
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
            logging.info("Read %d records, %d variables", len(ds.i), len(ds.data_vars))
            logging.info("Appending to %s", args.output)
            try:
                with xr.open_dataset(args.output) as ds_existing:
                    ds_combined = xr.concat([ds_existing, ds], dim="i")
                tmp_fd, tmp_path = tempfile.mkstemp(suffix=".nc", dir=args.output.parent)
                try:
                    import os

                    os.close(tmp_fd)
                    ds_combined.to_netcdf(
                        tmp_path, encoding=_nc_encoding(ds_combined, args.compression)
                    )
                    Path(tmp_path).replace(args.output)
                except Exception:
                    Path(tmp_path).unlink(missing_ok=True)
                    raise
            except (OSError, ValueError) as e:
                logging.error("Error appending to %s: %s", args.output, e)
                return 1
        else:
            try:
                import netCDF4  # noqa: F401

                has_netcdf4 = True
            except ImportError:
                has_netcdf4 = False

            if has_netcdf4:
                # Streaming mode: write directly to NetCDF without holding all data
                logging.info("Writing to %s (streaming)", args.output)
                n_records, n_files = xdbd.write_multi_dbd_netcdf(
                    args.files,
                    args.output,
                    skip_first_record=args.skipFirst,
                    repair=args.repair,
                    to_keep=to_keep,
                    criteria=criteria,
                    skip_missions=args.skipMission,
                    keep_missions=args.keepMission,
                    cache_dir=cache_dir,
                    compression=args.compression,
                )
                logging.info("Wrote %d records from %d files", n_records, n_files)
            else:
                # Fallback: load via xarray then write (works with scipy backend)
                logging.info("Writing to %s (netCDF4 not available, using xarray)", args.output)
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
                ds.to_netcdf(str(args.output), encoding=_nc_encoding(ds, args.compression))

        logging.info("Successfully wrote %s", args.output)
        return 0

    except (OSError, ValueError, RuntimeError) as e:
        logging.error("Error: %s", e)
        logging.debug("Traceback:", exc_info=True)
        return 1


def main():
    """Standalone entry point for dbd2nc."""
    parser = ArgumentParser(
        description="Convert Slocum glider DBD files to NetCDF format",
        epilog="Report bugs to pat@mousebrains.com",
    )
    _add_common_args(parser)
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {xdbd.__version__}",
    )
    args = parser.parse_args()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
