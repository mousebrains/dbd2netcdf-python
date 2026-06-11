#!/usr/bin/env python3
"""
Convert Slocum glider DBD files to NetCDF format.
"""

from __future__ import annotations

import logging
import os
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
        "--sensor-output",
        type=Path,
        metavar="filename",
        help="File containing sensors to output",
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
        help="Where to store the data (required unless --list-sensors)",
    )
    parser.add_argument(
        "--list-sensors",
        action="store_true",
        help="Print available sensors and exit (no conversion)",
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
        "--compression",
        type=int,
        default=5,
        metavar="level",
        help="NetCDF compression level 1-9 (default: 5, <=0 to disable)",
    )
    parser.add_argument(
        "--sort",
        choices=("lexicographic", "header_time", "none"),
        default="header_time",
        help="File sort order (default: header_time)",
    )
    logger.add_args(parser)


def add_args(subparsers) -> None:
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
    logger.mk_logger(args)

    for f in args.files:
        if not f.exists():
            logging.error("File not found: %s", f)
            return 1

    if getattr(args, "list_sensors", False):
        cache_dir = args.cache
        if cache_dir is None and len(args.files) > 0:
            cache_dir = args.files[0].parent / "cache"
        file_strs = [str(f) for f in args.files]
        cache_str = str(cache_dir) if cache_dir else ""
        result = xdbd.scan_sensors(
            file_strs,
            cache_dir=cache_str,
            skip_missions=getattr(args, "skip_mission", None) or [],
            keep_missions=getattr(args, "keep_mission", None) or [],
        )
        names = list(result["sensor_names"])
        units = list(result["sensor_units"])
        sizes = list(result["sensor_sizes"])
        for name, unit, sz in sorted(zip(names, units, sizes, strict=True)):
            print(f"{name:40s} {unit:15s} ({sz} bytes)")
        return 0

    if not getattr(args, "output", None):
        logging.error("--output is required (unless --list-sensors)")
        return 1

    criteria = None
    if args.sensors:
        if not args.sensors.exists():
            logging.error("Sensors file not found: %s", args.sensors)
            return 1
        criteria = read_sensor_list(args.sensors)
        logging.info("Loaded %d criteria sensors from %s", len(criteria), args.sensors)

    to_keep = None
    if args.sensor_output:
        if not args.sensor_output.exists():
            logging.error("Sensor output file not found: %s", args.sensor_output)
            return 1
        to_keep = read_sensor_list(args.sensor_output)
        logging.info("Loaded %d output sensors from %s", len(to_keep), args.sensor_output)

    cache_dir = args.cache
    if cache_dir is None and len(args.files) > 0:
        cache_dir = args.files[0].parent / "cache"
    if cache_dir and not cache_dir.exists():
        logging.warning("Cache directory not found: %s", cache_dir)
        cache_dir = None

    output_existed = args.output.exists()
    if output_existed and not args.append:
        logging.info("Overwriting existing file: %s", args.output)

    # Default: skip first record of every file (matches mkone and dbdreader).
    # --keep-first inverts; --skip-first is explicit (mutex group).
    skip_first = not args.keep_first

    try:
        logging.info("Processing %d file(s)...", len(args.files))

        if args.append and args.output.exists():
            # Append mode: load everything into memory to concatenate
            ds = xdbd.open_multi_dbd_dataset(
                args.files,
                skip_first_record=skip_first,
                repair=args.repair,
                to_keep=to_keep,
                criteria=criteria,
                skip_missions=args.skip_mission,
                keep_missions=args.keep_mission,
                cache_dir=cache_dir,
                sort=args.sort,
            )
            logging.info("Read %d records, %d variables", len(ds.i), len(ds.data_vars))
            logging.info("Appending to %s", args.output)
            try:
                with xr.open_dataset(args.output) as ds_existing:
                    ds_combined = xr.concat([ds_existing, ds], dim="i")
                tmp_fd, tmp_path = tempfile.mkstemp(suffix=".nc", dir=args.output.parent)
                try:
                    os.close(tmp_fd)
                    ds_combined.to_netcdf(
                        tmp_path, encoding=_nc_encoding(ds_combined, args.compression)
                    )
                    Path(tmp_path).replace(args.output)
                except (OSError, ValueError):
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
                    skip_first_record=skip_first,
                    repair=args.repair,
                    to_keep=to_keep,
                    criteria=criteria,
                    skip_missions=args.skip_mission,
                    keep_missions=args.keep_mission,
                    cache_dir=cache_dir,
                    compression=args.compression,
                    sort=args.sort,
                )
                logging.info("Wrote %d records from %d files", n_records, n_files)
            else:
                # Fallback: load via xarray then write (works with scipy backend)
                logging.info("Writing to %s (netCDF4 not available, using xarray)", args.output)
                ds = xdbd.open_multi_dbd_dataset(
                    args.files,
                    skip_first_record=skip_first,
                    repair=args.repair,
                    to_keep=to_keep,
                    criteria=criteria,
                    skip_missions=args.skip_mission,
                    keep_missions=args.keep_mission,
                    cache_dir=cache_dir,
                    sort=args.sort,
                )
                ds.to_netcdf(str(args.output), encoding=_nc_encoding(ds, args.compression))

        logging.info("Successfully wrote %s", args.output)
        return 0

    except (OSError, ValueError, RuntimeError) as e:
        # If we created the output file this run, unlink it so a partial/empty
        # NetCDF doesn't masquerade as a successful output on the next run.
        if not args.append and not output_existed and args.output.exists():
            try:
                args.output.unlink()
                logging.info("Removed incomplete output: %s", args.output)
            except OSError as unlink_err:
                logging.warning("Could not remove incomplete %s: %s", args.output, unlink_err)
        logging.error("Error: %s", e, exc_info=True)
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
