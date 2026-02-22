#!/usr/bin/env python3
"""
Advanced example: Working with glider data

Shows more advanced operations: subsetting by record index, filtering by
depth, computing derived quantities, and printing dataset info.

Usage:
    python advanced_usage.py -C cache/ *.dbd
"""

from argparse import ArgumentParser
from pathlib import Path

import numpy as np

import xarray_dbd as xdbd

parser = ArgumentParser(description="Advanced glider data exploration")
parser.add_argument("files", nargs="+", type=Path, help="DBD files to load")
parser.add_argument(
    "-C",
    "--cache",
    type=Path,
    metavar="directory",
    help="Sensor cache directory (default: <file_dir>/cache)",
)
args = parser.parse_args()

# Load data
ds = xdbd.open_multi_dbd_dataset(args.files, cache_dir=args.cache)

# Basic statistics
print("=== Data Summary ===")
print(f"Total records: {len(ds.i)}")
print(f"Variables: {len(ds.data_vars)}")
print("\nTime range:")
if "m_present_time" in ds:
    time_var = ds["m_present_time"]
    print(f"  Start: {time_var.min().values}")
    print(f"  End: {time_var.max().values}")

# Select data by record range
if "m_present_time" in ds:
    ds_subset = ds.isel(i=slice(0, len(ds.i) // 2))
    print(f"\nFirst-half subset has {len(ds_subset.i)} records")

# Filter by depth
if "m_depth" in ds:
    # Get only records deeper than 10m
    deep_records = ds.where(ds["m_depth"] > 10, drop=True)
    print(f"Records deeper than 10m: {len(deep_records.i)}")

# Calculate derived quantities
if "m_lat" in ds and "m_lon" in ds:
    # Calculate approximate distance traveled
    lat = ds["m_lat"].values
    lon = ds["m_lon"].values

    # Simple distance calculation (not accurate for long distances)
    dlat = np.diff(lat)
    dlon = np.diff(lon)
    distances = np.sqrt(dlat**2 + dlon**2) * 111.0  # rough km conversion

    total_distance = np.sum(distances)
    print(f"\nApproximate distance traveled: {total_distance:.2f} km")

# Print dataset info
print("\n=== Dataset Info ===")
print(ds)
