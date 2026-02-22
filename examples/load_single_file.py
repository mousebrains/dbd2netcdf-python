#!/usr/bin/env python3
"""
Simple example: Load a single DBD file

Shows how to load a single glider DBD file as an xarray Dataset, inspect
its dimensions and variables, and convert to a pandas DataFrame.

Usage:
    python load_single_file.py -C cache/ file.dbd
"""

from argparse import ArgumentParser
from pathlib import Path

import xarray as xr

parser = ArgumentParser(description="Load a single DBD file and print a summary")
parser.add_argument("file", type=Path, help="DBD file to load")
parser.add_argument(
    "-C",
    "--cache",
    type=Path,
    metavar="directory",
    help="Sensor cache directory (default: <file_dir>/cache)",
)
args = parser.parse_args()

# Load a single DBD file — two equivalent approaches:
#
#   ds = xdbd.open_dbd_dataset(args.file, cache_dir=args.cache)
#   ds = xr.open_dataset(args.file, engine="dbd", cache_dir=args.cache)
#
ds = xr.open_dataset(args.file, engine="dbd", cache_dir=args.cache)

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
