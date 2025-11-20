#!/usr/bin/env python3
"""Compare first record values with dbd2netCDF output"""

import sys
from pathlib import Path
import numpy as np
sys.path.insert(0, str(Path(__file__).parent))

# Read dbd2netCDF output
import xarray as xr
ds_ref = xr.open_dataset('/tmp/test.nc', decode_timedelta=False)

# Read our output
import xarray_dbd as xdbd
ds_ours = xdbd.open_dbd_dataset(Path('dbd_files/01330000.dcd'), skip_first_record=False)

print("Comparing first record values:")
print("=" * 80)

# Find some variables that should have data in the first record
test_vars = ['m_depth', 'm_lat', 'm_lon', 'm_gps_lat', 'm_water_depth']

for var in test_vars:
    if var in ds_ref and var in ds_ours:
        ref_val = ds_ref[var].values[0]
        our_val = ds_ours[var].values[0]

        print(f"\n{var}:")
        print(f"  dbd2netCDF: {ref_val}")
        print(f"  xarray-dbd: {our_val}")

        if not np.isnan(ref_val) and not np.isnan(our_val):
            if ref_val != 0:
                ratio = our_val / ref_val
                print(f"  Ratio: {ratio:.3e}")

# Also check dimensions
print(f"\n\nDimensions:")
print(f"  dbd2netCDF: i={len(ds_ref.i)}, j={len(ds_ref.j)}")
print(f"  xarray-dbd: i={len(ds_ours.i)}")

# Check total number of variables
print(f"\nVariables:")
print(f"  dbd2netCDF: {len(ds_ref.data_vars)}")
print(f"  xarray-dbd: {len(ds_ours.data_vars)}")
