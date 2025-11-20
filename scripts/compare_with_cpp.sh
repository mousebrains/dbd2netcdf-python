#!/bin/bash
#
# Compare Python dbd2nc output with C++ dbd2netCDF output
# Usage: ./scripts/compare_with_cpp.sh <dbd_file> [<dbd_file2> ...]
#

set -e

# Check for required commands
command -v python3 >/dev/null 2>&1 || { echo "Error: python3 not found"; exit 1; }

# Find C++ dbd2netCDF
CPP_DBD2NC="${DBD2NETCDF_BIN:-dbd2netcdf/bin/dbd2netCDF}"
if [ ! -f "$CPP_DBD2NC" ]; then
    echo "Error: C++ dbd2netCDF not found at: $CPP_DBD2NC"
    echo "Set DBD2NETCDF_BIN environment variable or build dbd2netCDF"
    exit 1
fi

# Check for input files
if [ $# -eq 0 ]; then
    echo "Usage: $0 <dbd_file> [<dbd_file2> ...]"
    echo ""
    echo "Example:"
    echo "  $0 dbd_files/01330000.dcd"
    echo "  $0 dbd_files/*.dcd"
    exit 1
fi

# Get cache directory
CACHE_DIR="${DBD_CACHE:-dbd_files/cache}"
if [ ! -d "$CACHE_DIR" ]; then
    echo "Warning: Cache directory not found: $CACHE_DIR"
    echo "Set DBD_CACHE environment variable if needed"
fi

# Create temp directory for outputs
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "================================================================"
echo "Comparing Python vs C++ dbd2netCDF Implementation"
echo "================================================================"
echo ""
echo "Files to process: $#"
echo "Cache directory: $CACHE_DIR"
echo "C++ binary: $CPP_DBD2NC"
echo ""

# Process each file
for INPUT in "$@"; do
    BASENAME=$(basename "$INPUT" | sed 's/\.[^.]*$//')

    echo "Processing: $INPUT"
    echo "  Basename: $BASENAME"

    # Run C++ version
    CPP_OUT="$TMPDIR/${BASENAME}_cpp.nc"
    echo "  Running C++ dbd2netCDF..."
    "$CPP_DBD2NC" -C "$CACHE_DIR" -o "$CPP_OUT" "$INPUT" 2>&1 | sed 's/^/    /'

    # Run Python version
    PY_OUT="$TMPDIR/${BASENAME}_py.nc"
    echo "  Running Python dbd2nc..."
    python3 dbd2nc.py -C "$CACHE_DIR" -o "$PY_OUT" "$INPUT" 2>&1 | sed 's/^/    /'

    # Compare outputs
    echo "  Comparing outputs..."
    python3 -c "
import xarray as xr
import numpy as np
import sys

ds_cpp = xr.open_dataset('$CPP_OUT', decode_timedelta=False)
ds_py = xr.open_dataset('$PY_OUT', decode_timedelta=False)

print('    C++ records: {}'.format(ds_cpp.sizes.get('i', 0)))
print('    Python records: {}'.format(ds_py.sizes.get('i', 0)))

if ds_cpp.sizes.get('i') != ds_py.sizes.get('i'):
    print('    ❌ FAIL: Record count mismatch')
    sys.exit(1)

common_vars = set(ds_cpp.data_vars) & set(ds_py.data_vars)
print('    Common variables: {}'.format(len(common_vars)))

diffs = []
for var in sorted(common_vars):
    data_cpp = np.squeeze(ds_cpp[var].values)
    data_py = np.squeeze(ds_py[var].values)

    if not np.array_equal(data_cpp, data_py, equal_nan=True):
        mask = ~(np.isnan(data_cpp) | np.isnan(data_py))
        if np.sum(mask) > 0:
            vals_cpp = data_cpp[mask]
            vals_py = data_py[mask]
            if not np.allclose(vals_cpp, vals_py, rtol=1e-9, atol=1e-12, equal_nan=True):
                diffs.append(var)

if diffs:
    print('    ❌ FAIL: {} variables differ'.format(len(diffs)))
    for var in diffs[:5]:
        print('      - {}'.format(var))
    if len(diffs) > 5:
        print('      ... and {} more'.format(len(diffs) - 5))
    sys.exit(1)
else:
    print('    ✓ PASS: All variables match')
" || { echo "  ❌ Comparison failed"; exit 1; }

    echo ""
done

echo "================================================================"
echo "✓ All comparisons passed!"
echo "================================================================"
