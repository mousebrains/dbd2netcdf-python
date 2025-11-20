#!/usr/bin/env python3
import xarray as xr
import subprocess
import tempfile
import numpy as np
from pathlib import Path
import os

# Test with .dcd file
test_file = Path('dbd_files/01220000.dcd')

with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp1:
    nc_cpp = tmp1.name
with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp2:
    nc_python = tmp2.name

# Run C++ version
subprocess.run([
    '/Users/pat/tpw/dbd2netcdf/bin/dbd2netCDF',
    '-C', 'dbd_files/cache',
    '-o', nc_cpp,
    str(test_file)
], capture_output=True)

# Run Python version
subprocess.run([
    'python3', 'dbd2nc.py',
    '-C', 'dbd_files/cache',
    '-o', nc_python,
    str(test_file)
], capture_output=True)

# Compare
ds1 = xr.open_dataset(nc_cpp, decode_timedelta=False)
ds2 = xr.open_dataset(nc_python, decode_timedelta=False)

common = set(ds1.data_vars) & set(ds2.data_vars)
print(f'Common variables: {len(common)}')

diffs = []
for var in common:
    data1 = np.squeeze(ds1[var].values)
    data2 = np.squeeze(ds2[var].values)

    if not np.array_equal(data1, data2, equal_nan=True):
        mask = ~(np.isnan(data1) | np.isnan(data2))
        if np.sum(mask) > 0:
            vals1 = data1[mask]
            vals2 = data2[mask]
            if not np.allclose(vals1, vals2, rtol=1e-9, atol=1e-12):
                max_diff = np.max(np.abs(vals1 - vals2))
                n_diff = np.count_nonzero(vals1 != vals2)
                diffs.append((var, max_diff, n_diff))

print(f'\nSignificant differences: {len(diffs)}')
for var, diff, n in diffs[:20]:
    print(f'  {var}: max diff = {diff:.3e}, {n} values differ')

os.unlink(nc_cpp)
os.unlink(nc_python)
