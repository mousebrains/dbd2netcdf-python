#!/usr/bin/env python3
"""
Advanced example: Working with glider data

This example shows more advanced operations like plotting, filtering,
and data analysis with xarray-dbd.
"""

from pathlib import Path

import numpy as np

import xarray_dbd as xdbd

# Load data
data_dir = Path("path/to/data")
files = sorted(data_dir.glob("*.dbd"))
ds = xdbd.open_multi_dbd_dataset(files)

# Basic statistics
print("=== Data Summary ===")
print(f"Total records: {len(ds.i)}")
print(f"Variables: {len(ds.data_vars)}")
print("\nTime range:")
if "m_present_time" in ds:
    time_var = ds["m_present_time"]
    print(f"  Start: {time_var.min().values}")
    print(f"  End: {time_var.max().values}")

# Select data by time range
if "m_present_time" in ds:
    # Get data from a specific time window
    start_time = ds["m_present_time"].values[0]
    end_time = ds["m_present_time"].values[len(ds.i) // 2]

    # Simple indexing by record number
    ds_subset = ds.isel(i=slice(0, len(ds.i) // 2))
    print(f"\nSubset has {len(ds_subset.i)} records")

# Filter by depth
if "m_depth" in ds:
    # Get only records deeper than 10m
    deep_records = ds.where(ds["m_depth"] > 10, drop=True)
    print(f"\nRecords deeper than 10m: {len(deep_records.i)}")

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

# Export to different formats
print("\n=== Exporting Data ===")

# NetCDF (most efficient for xarray)
ds.to_netcdf("data.nc")
print("Saved to data.nc")

# CSV (via pandas)
df = ds.to_dataframe()
df.to_csv("data.csv")
print("Saved to data.csv")

# Select specific variables for export
if "m_depth" in ds and "m_lat" in ds and "m_lon" in ds:
    subset = ds[["m_depth", "m_lat", "m_lon"]]
    subset.to_netcdf("positions.nc")
    print("Saved position data to positions.nc")

# Print data info
print("\n=== Dataset Info ===")
print(ds)
