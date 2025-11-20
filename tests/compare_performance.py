#!/usr/bin/env python3
"""
Comprehensive performance and correctness comparison
"""

import sys
import time
import numpy as np
from pathlib import Path
import subprocess
import tracemalloc

sys.path.insert(0, str(Path(__file__).parent))

import xarray_dbd as xdbd


def test_performance_single_file():
    """Test performance on a single large file"""
    print("=" * 70)
    print("Performance Test: Single Large File")
    print("=" * 70)

    test_file = Path("dbd_files/01330000.dcd")
    if not test_file.exists():
        print(f"✗ Test file not found: {test_file}")
        return

    file_size_mb = test_file.stat().st_size / (1024 * 1024)
    print(f"File: {test_file.name} ({file_size_mb:.2f} MB)")

    # Test xarray-dbd
    print("\n1. xarray-dbd performance:")
    tracemalloc.start()
    start = time.time()

    ds = xdbd.open_dbd_dataset(test_file, skip_first_record=False)

    elapsed = time.time() - start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    n_records = len(ds.i)
    n_vars = len(ds.data_vars)

    print(f"   Time: {elapsed:.3f} seconds")
    print(f"   Throughput: {file_size_mb / elapsed:.1f} MB/s")
    print(f"   Records: {n_records}")
    print(f"   Variables: {n_vars}")
    print(f"   Memory (peak): {peak / (1024 * 1024):.1f} MB")

    # List some data stats
    print(f"\n   Sample data check:")
    vars_with_data = []
    for var in list(ds.data_vars)[:20]:
        data = ds[var].values
        n_valid = np.sum(~np.isnan(data))
        if n_valid > 0:
            vars_with_data.append((var, n_valid, data[~np.isnan(data)].mean()))

    for var, n, mean in vars_with_data[:5]:
        print(f"     {var:30s}: {n:4d} values, mean={mean:.3g}")


def test_performance_multiple_files():
    """Test performance on multiple files"""
    print("\n" + "=" * 70)
    print("Performance Test: Multiple Files")
    print("=" * 70)

    files = sorted(Path("dbd_files").glob("*.dcd"))[:10]
    if not files:
        print("✗ No .dcd files found")
        return

    total_size = sum(f.stat().st_size for f in files) / (1024 * 1024)
    print(f"Files: {len(files)} files, {total_size:.2f} MB total")

    # Test xarray-dbd
    print("\n1. xarray-dbd multi-file performance:")
    tracemalloc.start()
    start = time.time()

    ds = xdbd.open_multi_dbd_dataset(files, skip_first_record=False)

    elapsed = time.time() - start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    n_records = len(ds.i)
    n_vars = len(ds.data_vars)

    print(f"   Time: {elapsed:.3f} seconds")
    print(f"   Throughput: {total_size / elapsed:.1f} MB/s")
    print(f"   Total records: {n_records}")
    print(f"   Variables: {n_vars}")
    print(f"   Memory (peak): {peak / (1024 * 1024):.1f} MB")
    print(f"   Records/file: {n_records / len(files):.1f} avg")


def test_data_correctness():
    """Test that data values are reasonable"""
    print("\n" + "=" * 70)
    print("Data Correctness Test")
    print("=" * 70)

    test_file = Path("dbd_files/01330000.dcd")
    if not test_file.exists():
        print("✗ Test file not found")
        return

    ds = xdbd.open_dbd_dataset(test_file, skip_first_record=False)

    print(f"Dataset info:")
    print(f"   Records: {len(ds.i)}")
    print(f"   Variables: {len(ds.data_vars)}")

    # Check for reasonable data
    issues = []
    vars_checked = 0
    vars_with_data = 0

    for var in ds.data_vars:
        vars_checked += 1
        data = ds[var].values

        n_valid = np.sum(~np.isnan(data))
        n_inf = np.sum(np.isinf(data))

        if n_valid > 0:
            vars_with_data += 1

        if n_inf > 0:
            issues.append(f"{var}: {n_inf} infinite values")

        if n_valid > 0:
            valid_data = data[~np.isnan(data)]
            if np.any(np.abs(valid_data) > 1e10):
                issues.append(f"{var}: very large values (max={np.max(np.abs(valid_data)):.3g})")

    print(f"\n   Variables checked: {vars_checked}")
    print(f"   Variables with data: {vars_with_data}")
    print(f"   Issues found: {len(issues)}")

    if issues and len(issues) <= 10:
        for issue in issues[:10]:
            print(f"     - {issue}")
    elif issues:
        print(f"     (showing first 10/{len(issues)} issues)")
        for issue in issues[:10]:
            print(f"     - {issue}")

    if not issues:
        print("   ✓ No data issues detected")


def main():
    """Run all tests"""
    print("\nxarray-dbd Performance & Correctness Suite")
    print("=" * 70)

    tests = [
        test_performance_single_file,
        test_performance_multiple_files,
        test_data_correctness,
    ]

    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"\n✗ Test failed: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 70)
    print("Testing complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
