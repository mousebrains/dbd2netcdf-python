#!/usr/bin/env python3
"""
Test script for xarray-dbd engine
"""

import sys
import time
from pathlib import Path

import numpy as np

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

import xarray_dbd as xdbd


def test_single_file():
    """Test reading a single DBD file"""
    print("=" * 60)
    print("Test 1: Reading single DBD file")
    print("=" * 60)

    test_file = Path("dbd2netcdf/test/test.sbd")
    if not test_file.exists():
        print(f"ERROR: Test file {test_file} not found")
        return False

    try:
        start = time.time()
        ds = xdbd.open_dbd_dataset(test_file)
        elapsed = time.time() - start

        print(f"✓ Successfully opened {test_file}")
        print(f"  Time: {elapsed:.3f} seconds")
        print(f"  Records: {len(ds.i)}")
        print(f"  Variables: {len(ds.data_vars)}")
        print(f"  Dimensions: {dict(ds.dims)}")

        # Print some variables
        print("\n  First 5 variables:")
        for _i, var in enumerate(list(ds.data_vars)[:5]):
            units = ds[var].attrs.get('units', '')
            print(f"    {var}: {units}")

        # Print attributes
        print("\n  Attributes:")
        for key in ['mission_name', 'encoding_version', 'source_file']:
            if key in ds.attrs:
                print(f"    {key}: {ds.attrs[key]}")

        # Check data
        print("\n  Data check:")
        for var in list(ds.data_vars)[:3]:
            data = ds[var].values
            n_valid = np.sum(~np.isnan(data))
            print(f"    {var}: {n_valid}/{len(data)} valid values")

        return True

    except Exception as e:
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multiple_files():
    """Test reading multiple DBD files"""
    print("\n" + "=" * 60)
    print("Test 2: Reading multiple DBD files")
    print("=" * 60)

    test_files = [
        Path("dbd2netcdf/test/test.sbd"),
        Path("dbd2netcdf/test/test.tbd"),
    ]

    # Filter to existing files
    test_files = [f for f in test_files if f.exists()]

    if not test_files:
        print("ERROR: No test files found")
        return False

    try:
        start = time.time()
        ds = xdbd.open_multi_dbd_dataset(test_files)
        elapsed = time.time() - start

        print(f"✓ Successfully opened {len(test_files)} files")
        print(f"  Time: {elapsed:.3f} seconds")
        print(f"  Total records: {len(ds.i)}")
        print(f"  Variables: {len(ds.data_vars)}")
        print(f"  Files processed: {ds.attrs.get('n_files', 0)}")

        return True

    except Exception as e:
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sensor_filtering():
    """Test sensor filtering"""
    print("\n" + "=" * 60)
    print("Test 3: Sensor filtering")
    print("=" * 60)

    test_file = Path("dbd2netcdf/test/test.sbd")
    if not test_file.exists():
        print(f"ERROR: Test file {test_file} not found")
        return False

    try:
        # First get all sensors
        ds_all = xdbd.open_dbd_dataset(test_file)
        all_vars = list(ds_all.data_vars)
        print(f"Total variables: {len(all_vars)}")

        # Filter to subset
        to_keep = all_vars[:5]  # Keep first 5
        print(f"Filtering to: {to_keep}")

        ds_filtered = xdbd.open_dbd_dataset(test_file, to_keep=to_keep)
        filtered_vars = list(ds_filtered.data_vars)

        print(f"✓ Filtered variables: {filtered_vars}")
        print(f"  Expected: {len(to_keep)}, Got: {len(filtered_vars)}")

        if set(filtered_vars) == set(to_keep):
            print("  ✓ Filtering successful")
            return True
        else:
            print("  ✗ Filtering mismatch")
            return False

    except Exception as e:
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_data_values():
    """Test that data values are reasonable"""
    print("\n" + "=" * 60)
    print("Test 4: Data value validation")
    print("=" * 60)

    test_file = Path("dbd2netcdf/test/test.sbd")
    if not test_file.exists():
        print(f"ERROR: Test file {test_file} not found")
        return False

    try:
        ds = xdbd.open_dbd_dataset(test_file)

        print("Checking data values...")
        all_ok = True

        for var in list(ds.data_vars)[:10]:  # Check first 10
            data = ds[var].values
            n_valid = np.sum(~np.isnan(data))
            n_inf = np.sum(np.isinf(data))

            print(f"  {var}:")
            print(f"    Valid: {n_valid}/{len(data)}")
            print(f"    Inf: {n_inf}")

            if n_inf > 0:
                print(f"    ✗ WARNING: Found {n_inf} infinite values")
                all_ok = False

            if n_valid > 0:
                valid_data = data[~np.isnan(data)]
                print(f"    Range: [{np.min(valid_data):.6g}, {np.max(valid_data):.6g}]")

        if all_ok:
            print("\n✓ All data values look reasonable")
            return True
        else:
            print("\n⚠ Some data issues found")
            return False

    except Exception as e:
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_performance():
    """Compare performance with reference"""
    print("\n" + "=" * 60)
    print("Test 5: Performance check")
    print("=" * 60)

    test_file = Path("dbd2netcdf/test/test.sbd")
    if not test_file.exists():
        print(f"ERROR: Test file {test_file} not found")
        return False

    try:
        # Multiple runs for averaging
        n_runs = 5
        times = []

        print(f"Running {n_runs} iterations...")
        for i in range(n_runs):
            start = time.time()
            ds = xdbd.open_dbd_dataset(test_file)
            # Force data load
            _ = ds['m_present_time'].values if 'm_present_time' in ds else None
            elapsed = time.time() - start
            times.append(elapsed)
            print(f"  Run {i+1}: {elapsed:.3f}s")

        avg_time = np.mean(times)
        std_time = np.std(times)

        print(f"\n✓ Average time: {avg_time:.3f} ± {std_time:.3f}s")
        print(f"  File size: {test_file.stat().st_size / 1024:.1f} KB")
        print(f"  Records: {len(ds.i)}")
        print(f"  Throughput: {len(ds.i) / avg_time:.0f} records/sec")

        return True

    except Exception as e:
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests"""
    print("\nxarray-dbd Test Suite")
    print("=" * 60)

    tests = [
        test_single_file,
        test_multiple_files,
        test_sensor_filtering,
        test_data_values,
        test_performance,
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"\n✗ Test failed with exception: {e}")
            import traceback
            traceback.print_exc()
            results.append(False)

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Passed: {passed}/{total}")

    if passed == total:
        print("✓ All tests passed!")
        return 0
    else:
        print(f"✗ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
