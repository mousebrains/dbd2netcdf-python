#!/usr/bin/env python3
"""
Simple example: Load a single DBD file

This example shows how to load a single glider DBD file as an xarray Dataset.
"""

import xarray_dbd as xdbd

# Load a single DBD file
ds = xdbd.open_dbd_dataset("path/to/your/file.dbd")

# Display basic information
print("Dataset dimensions:", dict(ds.dims))
print("\nAvailable variables:")
for var in ds.data_vars:
    print(f"  - {var}: {ds[var].attrs.get('units', 'no units')}")

# Access specific variables
if "m_depth" in ds:
    depth = ds["m_depth"]
    print(f"\nDepth range: {depth.min().values:.2f} to {depth.max().values:.2f} m")

if "m_lat" in ds and "m_lon" in ds:
    print(f"Lat range: {ds['m_lat'].min().values:.4f} to {ds['m_lat'].max().values:.4f}")
    print(f"Lon range: {ds['m_lon'].min().values:.4f} to {ds['m_lon'].max().values:.4f}")

# Convert to pandas DataFrame if needed
df = ds.to_dataframe()
print(f"\nDataFrame shape: {df.shape}")

# Save to NetCDF
ds.to_netcdf("output.nc")
print("\nSaved to output.nc")
