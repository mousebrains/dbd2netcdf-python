#!/usr/bin/env python3
"""
Unified CLI for xarray-dbd: xdbd

Subcommands:
    2nc      — Convert DBD files to NetCDF
    2csv     — Convert DBD files to CSV
    sensors  — List sensors from DBD file headers
    mkone    — Batch process directories of DBD files
"""

import sys
from argparse import ArgumentParser

import xarray_dbd as xdbd
from xarray_dbd.cli import cache, csv, dbd2nc, missions, mkone, sensors


def main():
    parser = ArgumentParser(
        prog="xdbd",
        description="xarray-dbd command-line tools for Slocum glider DBD files",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {xdbd.__version__}",
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    dbd2nc.add_args(subparsers)
    csv.add_args(subparsers)
    sensors.add_args(subparsers)
    missions.add_args(subparsers)
    cache.add_args(subparsers)
    mkone.add_args(subparsers)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
