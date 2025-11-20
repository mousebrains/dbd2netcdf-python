#!/usr/bin/env python3
"""
Test dbd2nc.py against dbd2netCDF for various file types
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import xarray as xr


def compare_netcdf_files(file1: Path, file2: Path, verbose: bool = True) -> bool:
    """Compare two NetCDF files for equivalence"""

    if verbose:
        print("\nComparing:")
        print(f"  File 1: {file1}")
        print(f"  File 2: {file2}")

    # Open both files
    ds1 = xr.open_dataset(file1, decode_timedelta=False)
    ds2 = xr.open_dataset(file2, decode_timedelta=False)

    # Compare dimensions (focus on 'i' dimension which contains the data)
    # The 'j' dimension is a legacy artifact with size 1 that may or may not be present
    if ds1.sizes.get('i') != ds2.sizes.get('i'):
        print("  ✗ 'i' dimension sizes differ:")
        print(f"    File 1: {ds1.sizes.get('i')}")
        print(f"    File 2: {ds2.sizes.get('i')}")
        return False

    if verbose:
        print(f"  ✓ Record count matches: i={ds1.sizes.get('i')}")

    # Get common variables
    vars1 = set(ds1.data_vars)
    vars2 = set(ds2.data_vars)

    only_in_1 = vars1 - vars2
    only_in_2 = vars2 - vars1
    common = vars1 & vars2

    if only_in_1:
        print(f"  Note: {len(only_in_1)} variables only in file 1: {sorted(only_in_1)[:5]}...")
    if only_in_2:
        print(f"  Note: {len(only_in_2)} variables only in file 2: {sorted(only_in_2)[:5]}...")

    if verbose:
        print(f"  Comparing {len(common)} common variables...")

    # Compare each common variable
    differences = []
    for var in sorted(common):
        data1 = ds1[var].values
        data2 = ds2[var].values

        # Squeeze out singleton dimensions for comparison
        data1_squeezed = np.squeeze(data1)
        data2_squeezed = np.squeeze(data2)

        # Check shape (after squeezing)
        if data1_squeezed.shape != data2_squeezed.shape:
            differences.append(f"{var}: shape mismatch {data1_squeezed.shape} vs {data2_squeezed.shape}")
            continue

        # Use squeezed data for comparison
        data1 = data1_squeezed
        data2 = data2_squeezed

        # Compare values (accounting for NaN)
        mask1 = np.isnan(data1)
        mask2 = np.isnan(data2)

        if not np.array_equal(mask1, mask2):
            n_diff = np.sum(mask1 != mask2)
            differences.append(f"{var}: NaN pattern differs ({n_diff} positions)")
            continue

        # Compare non-NaN values
        valid_mask = ~mask1
        if np.sum(valid_mask) > 0:
            vals1 = data1[valid_mask]
            vals2 = data2[valid_mask]

            # Use appropriate comparison based on dtype
            if np.issubdtype(data1.dtype, np.floating):
                # For floats, use close comparison
                if not np.allclose(vals1, vals2, rtol=1e-9, atol=1e-12, equal_nan=True):
                    max_diff = np.max(np.abs(vals1 - vals2))
                    differences.append(f"{var}: values differ (max diff={max_diff:.3e})")
            else:
                # For integers, use exact comparison
                if not np.array_equal(vals1, vals2):
                    n_diff = np.sum(vals1 != vals2)
                    differences.append(f"{var}: {n_diff} values differ")

    ds1.close()
    ds2.close()

    if differences:
        print(f"  ✗ Found {len(differences)} differences:")
        for diff in differences[:10]:
            print(f"    - {diff}")
        if len(differences) > 10:
            print(f"    ... and {len(differences) - 10} more")
        return False

    if verbose:
        print(f"  ✓ All {len(common)} variables match!")

    return True


def test_file_type(file_pattern: str, description: str):
    """Test a specific file type"""
    print("\n" + "=" * 70)
    print(f"Testing {description}")
    print("=" * 70)

    # Find files
    files = sorted(Path("dbd_files").glob(file_pattern))
    if not files:
        print(f"No files found matching {file_pattern}")
        return False

    print(f"Found {len(files)} file(s)")

    # Use first file for single file test
    test_file = files[0]
    print(f"\nTest 1: Single file ({test_file.name})")

    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp1:
        nc_cpp = tmp1.name
    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp2:
        nc_python = tmp2.name

    try:
        # Run C++ version
        cmd_cpp = [
            "/Users/pat/tpw/dbd2netcdf/bin/dbd2netCDF",
            "-C", "dbd_files/cache",
            "-o", nc_cpp,
            str(test_file)
        ]
        result = subprocess.run(cmd_cpp, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  ✗ C++ dbd2netCDF failed: {result.stderr}")
            return False
        print("  ✓ C++ dbd2netCDF completed")

        # Run Python version
        cmd_python = [
            "python3", "dbd2nc.py",
            "-C", "dbd_files/cache",
            "-o", nc_python,
            str(test_file)
        ]
        result = subprocess.run(cmd_python, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"  ✗ Python dbd2nc failed: {result.stdout}\n{result.stderr}")
            return False
        print("  ✓ Python dbd2nc completed")

        # Compare outputs
        match = compare_netcdf_files(Path(nc_cpp), Path(nc_python))

    finally:
        # Cleanup
        for f in [nc_cpp, nc_python]:
            if os.path.exists(f):
                os.unlink(f)

    if not match:
        return False

    # Test with multiple files if available
    if len(files) >= 3:
        print(f"\nTest 2: Multiple files ({len(files[:3])} files)")

        with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp1:
            nc_cpp = tmp1.name
        with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp2:
            nc_python = tmp2.name

        try:
            # Run C++ version
            cmd_cpp = [
                "/Users/pat/tpw/dbd2netcdf/bin/dbd2netCDF",
                "-C", "dbd_files/cache",
                "-o", nc_cpp
            ] + [str(f) for f in files[:3]]

            result = subprocess.run(cmd_cpp, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"  ✗ C++ dbd2netCDF failed: {result.stderr}")
                return False
            print("  ✓ C++ dbd2netCDF completed")

            # Run Python version
            cmd_python = [
                "python3", "dbd2nc.py",
                "-C", "dbd_files/cache",
                "-o", nc_python
            ] + [str(f) for f in files[:3]]

            result = subprocess.run(cmd_python, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"  ✗ Python dbd2nc failed: {result.stdout}\n{result.stderr}")
                return False
            print("  ✓ Python dbd2nc completed")

            # Compare outputs
            match = compare_netcdf_files(Path(nc_cpp), Path(nc_python))

        finally:
            # Cleanup
            for f in [nc_cpp, nc_python]:
                if os.path.exists(f):
                    os.unlink(f)

        if not match:
            return False

    return True


def main():
    print("DBD to NetCDF Comparison Test Suite")
    print("=" * 70)

    # Change to working directory
    os.chdir("/Users/pat/tpw")

    results = {}

    # Test each file type
    tests = [
        ("*.dcd", "Compressed DBD files (.dcd)"),
        ("*.ecd", "Compressed EBD files (.ecd)"),
        ("*.tcd", "Compressed TBD files (.tcd)"),
        ("*.scd", "Compressed SBD files (.scd)"),
    ]

    for pattern, description in tests:
        results[description] = test_file_type(pattern, description)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    for test, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test}")

    all_passed = all(results.values())
    if all_passed:
        print("\n✓ All tests passed!")
        return 0
    else:
        print("\n✗ Some tests failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
