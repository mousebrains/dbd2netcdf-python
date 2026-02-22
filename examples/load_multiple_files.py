#!/usr/bin/env python3
"""
Simple example: Load multiple DBD files

Shows several ways to load and concatenate glider files: all at once,
with sensor filtering, and with mission filtering.

Usage:
    python load_multiple_files.py -C cache/ *.dbd
"""

from argparse import ArgumentParser
from pathlib import Path

import xarray_dbd as xdbd

parser = ArgumentParser(description="Load multiple DBD files and print a summary")
parser.add_argument("files", nargs="+", type=Path, help="DBD files to load")
parser.add_argument(
    "-C",
    "--cache",
    type=Path,
    metavar="directory",
    help="Sensor cache directory (default: <file_dir>/cache)",
)
args = parser.parse_args()

cache_dir = args.cache

# Method 1: Load all files and concatenate them
print(f"Loading {len(args.files)} files ...")
ds = xdbd.open_multi_dbd_dataset(args.files, cache_dir=cache_dir)

print(f"  {len(ds.i)} total records, {len(ds.data_vars)} variables")
print(f"  Variables: {list(ds.data_vars)[:10]}{'...' if len(ds.data_vars) > 10 else ''}")

# Method 2: Load files with sensor filtering — only keep certain sensors
sensors_to_keep = [
    "m_present_time",
    "m_depth",
    "m_lat",
    "m_lon",
    "sci_water_temp",
]

ds_filtered = xdbd.open_multi_dbd_dataset(
    args.files,
    to_keep=sensors_to_keep,
    cache_dir=cache_dir,
)

print(f"\nFiltered dataset has {len(ds_filtered.data_vars)} variables")

# Method 3: Skip certain missions
ds_no_test = xdbd.open_multi_dbd_dataset(
    args.files,
    skip_missions=["initial.mi", "status.mi"],
    cache_dir=cache_dir,
)

print(f"After skipping test missions: {len(ds_no_test.i)} records")

# Access the combined data
if "m_depth" in ds:
    print(f"\nDepth range: {ds['m_depth'].min().values:.2f} to {ds['m_depth'].max().values:.2f} m")
