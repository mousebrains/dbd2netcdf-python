#!/usr/bin/env python3
"""
Test xarray-dbd with real compressed DBD files
"""

import sys
import time
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import xarray_dbd as xdbd
from xarray_dbd.decompression import is_compressed, open_dbd_file


def test_decompression():
    """Test LZ4 decompression"""
    print("=" * 60)
    print("Test 1: LZ4 Decompression")
    print("=" * 60)

    test_file = Path("dbd_files/01330000.dcd")
    if not test_file.exists():
        print(f"✗ Test file {test_file} not found")
        return False

    try:
        # Check file is recognized as compressed
        assert is_compressed(test_file), "File not recognized as compressed"
        print(f"✓ File recognized as compressed")

        # Open and read some data
        with open_dbd_file(test_file, 'rb') as fp:
            # Read header lines
            lines = []
            for i in range(20):
                line = fp.readline()
                if not line:
                    break
                try:
                    lines.append(line.decode('ascii').strip())
                except UnicodeDecodeError:
                    break

            print(f"✓ Read {len(lines)} header lines")
            print(f"  First line: {lines[0] if lines else 'none'}")

            if len(lines) > 1:
                print(f"  Second line: {lines[1]}")

        return True

    except Exception as e:
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_read_compressed_file():
    """Test reading a compressed DBD file"""
    print("\n" + "=" * 60)
    print("Test 2: Read Compressed DBD File")
    print("=" * 60)

    test_file = Path("dbd_files/01330000.dcd")
    if not test_file.exists():
        print(f"✗ Test file {test_file} not found")
        return False

    try:
        start = time.time()
        ds = xdbd.open_dbd_dataset(test_file, skip_first_record=False)
        elapsed = time.time() - start

        print(f"✓ Successfully opened {test_file.name}")
        print(f"  Time: {elapsed:.3f} seconds")
        print(f"  Records: {len(ds.i)}")
        print(f"  Variables: {len(ds.data_vars)}")

        # List some variables
        vars_list = list(ds.data_vars)
        print(f"\n  First 10 variables:")
        for var in vars_list[:10]:
            data = ds[var].values
            n_valid = np.sum(~np.isnan(data))
            if n_valid > 0:
                valid_data = data[~np.isnan(data)]
                print(f"    {var:30s}: {n_valid:4d} values, range [{valid_data.min():.3g}, {valid_data.max():.3g}]")
            else:
                print(f"    {var:30s}: {n_valid:4d} values (all NaN)")

        # Check metadata
        print(f"\n  Metadata:")
        for key in ['mission_name', 'encoding_version', 'source_file']:
            if key in ds.attrs:
                print(f"    {key}: {ds.attrs[key]}")

        return True

    except Exception as e:
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multiple_compressed_files():
    """Test reading multiple compressed files"""
    print("\n" + "=" * 60)
    print("Test 3: Read Multiple Compressed Files")
    print("=" * 60)

    # Get first 5 dcd files
    files = sorted(Path("dbd_files").glob("*.dcd"))[:5]

    if not files:
        print("✗ No .dcd files found")
        return False

    print(f"Found {len(files)} files:")
    for f in files:
        print(f"  {f.name} ({f.stat().st_size / 1024:.1f} KB)")

    try:
        start = time.time()
        ds = xdbd.open_multi_dbd_dataset(files, skip_first_record=False)
        elapsed = time.time() - start

        print(f"\n✓ Successfully opened {len(files)} files")
        print(f"  Time: {elapsed:.3f} seconds")
        print(f"  Total records: {len(ds.i)}")
        print(f"  Variables: {len(ds.data_vars)}")
        print(f"  Files processed: {ds.attrs.get('n_files', 0)}")

        # Check some data
        vars_with_data = []
        for var in list(ds.data_vars)[:20]:
            data = ds[var].values
            n_valid = np.sum(~np.isnan(data))
            if n_valid > 0:
                vars_with_data.append((var, n_valid))

        print(f"\n  Variables with data (first 10):")
        for var, n_valid in vars_with_data[:10]:
            print(f"    {var:30s}: {n_valid} valid values")

        return True

    except Exception as e:
        print(f"✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_cache_sensors():
    """Test that sensor cache can be used"""
    print("\n" + "=" * 60)
    print("Test 4: Sensor Cache Usage")
    print("=" * 60)

    # Check cache files
    cache_dir = Path("dbd_files/cache")
    cache_files = list(cache_dir.glob("*.cac"))

    print(f"Found {len(cache_files)} cache files in {cache_dir}")

    if cache_files:
        # Show first few
        for cf in cache_files[:5]:
            print(f"  {cf.name}")

        # Try reading one
        try:
            with open(cache_files[0], 'r') as fp:
                lines = fp.readlines()[:5]
            print(f"\n  Sample cache file content ({cache_files[0].name}):")
            for line in lines:
                print(f"    {line.strip()}")
        except Exception as e:
            print(f"  (Could not read cache file: {e})")

    return True


def main():
    """Run all tests"""
    print("\nxarray-dbd Real Files Test Suite")
    print("=" * 60)

    tests = [
        test_decompression,
        test_read_compressed_file,
        test_multiple_compressed_files,
        test_cache_sensors,
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
