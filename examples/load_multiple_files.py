#!/usr/bin/env python3
"""
Simple example: Load multiple DBD files matching a pattern

This example shows how to load and concatenate multiple glider files.
"""

from pathlib import Path

import xarray_dbd as xdbd

# Method 1: Load all files in a directory matching a pattern
data_dir = Path("path/to/data")
dbd_files = sorted(data_dir.glob("*.dbd"))

print(f"Found {len(dbd_files)} files")

# Load all files and concatenate them
ds = xdbd.open_multi_dbd_dataset(dbd_files)

print(f"\nLoaded {len(ds.i)} total records from {len(dbd_files)} files")
print(f"Variables: {list(ds.data_vars)}")

# Method 2: Load specific file types
# Load only science data files (.ebd)
ebd_files = sorted(data_dir.glob("*.ebd"))
if ebd_files:
    ds_science = xdbd.open_multi_dbd_dataset(ebd_files)
    print(f"\nScience data: {len(ds_science.i)} records")

# Method 3: Load files with filtering
# Only keep certain sensors
sensors_to_keep = [
    "m_present_time",
    "m_depth",
    "m_lat",
    "m_lon",
    "sci_water_temp",
]

ds_filtered = xdbd.open_multi_dbd_dataset(
    dbd_files,
    to_keep=sensors_to_keep,
)

print(f"\nFiltered dataset has {len(ds_filtered.data_vars)} variables")

# Method 4: Skip certain missions
ds_no_test = xdbd.open_multi_dbd_dataset(
    dbd_files,
    skip_missions=["initial.mi", "status.mi"],
)

# Access the combined data
if "m_depth" in ds:
    print(f"\nDepth range: {ds['m_depth'].min().values:.2f} to {ds['m_depth'].max().values:.2f} m")
    print(f"Total records: {len(ds.i)}")

# Save concatenated data
ds.to_netcdf("combined_output.nc")
print("\nSaved combined data to combined_output.nc")
