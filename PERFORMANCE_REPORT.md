# Performance Comparison: C++ dbd2netCDF vs Python dbd2nc

## Executive Summary

Performance comparison between the C++ `dbd2netCDF` and Python `dbd2nc` implementations on real glider data files.

**Test Dataset:**
- **`.dcd` files**: 18 files, 5.52 MB total (compressed flight data)
- **`.ecd` files**: 18 files, 2.63 MB total (compressed science data)

## Results Summary

### ✅ .dcd Files (Flight Data) - BOTH SUCCEEDED

| Metric | C++ dbd2netCDF | Python dbd2nc | Ratio (Python/C++) |
|--------|----------------|---------------|-------------------|
| **Time** | 1.87 seconds | 6.70 seconds | **3.59x slower** |
| **Peak Memory** | 309 MB | 1,175 MB | **3.80x more** |
| **Output Size** | 9.0 MB | 258 MB | **28.6x larger** ⚠️ |
| **Records** | 18,071 | 18,071 | ✓ Same |

### ⚠️ .ecd Files (Science Data) - BOTH FAILED

| Metric | C++ dbd2netCDF | Python dbd2nc |
|--------|----------------|---------------|
| **Result** | Created empty file (0 records) | Error (return code 1) |
| **Time** | 0.04 seconds | 0.45 seconds |
| **Issue** | Missing cache file `91a09fa4.cac` | Missing cache file `91a09fa4.cac` |

## Detailed Analysis

### Performance: .dcd Files

The Python implementation is approximately **3.6x slower** and uses **3.8x more memory** than the C++ version for processing flight data files.

**Speed Breakdown:**
- C++: 1.87 seconds = 2.95 MB/s throughput
- Python: 6.70 seconds = 0.82 MB/s throughput

**Memory Profile:**
- C++: 309 MB peak (56x input size)
- Python: 1,175 MB peak (213x input size)

While the Python version is slower and uses more memory, it still completes the task in under 7 seconds for 18 files, which is acceptable for most use cases.

### Critical Issue: Data Type Preservation ⚠️

The Python implementation has a **serious bug** in data type handling when writing to NetCDF:

**Problem:**
All variables are stored as `double` (float64, 8 bytes) regardless of their actual type, resulting in output files that are **28.6x larger** than necessary.

**Evidence:**
```
C++ output (correct):
  byte cc_behavior_state(i) ;        // 1 byte
  float cc_bpump_value(i) ;          // 4 bytes

Python output (incorrect):
  double cc_behavior_state(i) ;      // 8 bytes
  double cc_bpump_value(i) ;         // 8 bytes
```

**Impact:**
- **Storage**: 258 MB vs 9 MB (28.6x waste)
- **I/O**: Slower read/write operations
- **Network**: Inefficient data transfer
- **Compatibility**: Different file structure than C++ version

**Root Cause:**
The Python backend (`xarray_dbd/backend.py`) reads sensor types correctly but doesn't use them when creating NetCDF variables. All arrays are converted to float64 by default.

**Fix Required:**
The backend needs to be updated to:
1. Use `sensor.dtype` when creating xarray Variables
2. Map DBD sensor types (1/2/4/8 bytes) to appropriate NumPy dtypes
3. Preserve byte/int16/float32/float64 types in NetCDF output

### .ecd Files Issue

Both implementations failed to process the `.ecd` files due to a missing cache file (`91a09fa4.cac`).

**Behavior Differences:**
- **C++**: Silently creates empty output file (0 records), returns success code
- **Python**: Warns about missing cache, attempts to read sensors from file, fails with error

**Resolution:**
The `.ecd` files use a factored sensor list that requires a cache file not present in the test dataset. This is expected behavior when processing files from a different deployment than what generated the cache.

## Performance Implications

### When to Use Python vs C++

**Use Python when:**
- ✓ Development speed matters more than runtime performance
- ✓ Integration with Python data analysis workflows (xarray, pandas)
- ✓ Processing small to medium datasets (< 1000 files)
- ✓ You need the xarray backend for direct file reading
- ⚠️ **AFTER** the data type bug is fixed

**Use C++ when:**
- ✓ Processing large datasets (1000s of files)
- ✓ Minimal memory footprint is critical
- ✓ Maximum processing speed required
- ✓ Production batch processing pipelines
- ✓ Need correct data types in output

### Projected Performance at Scale

Extrapolating to larger datasets:

**1,000 .dcd files (306 MB):**
- C++: ~104 seconds (1.7 minutes), ~17 GB peak memory
- Python: ~372 seconds (6.2 minutes), ~65 GB peak memory

**10,000 .dcd files (3.06 GB):**
- C++: ~1,039 seconds (17.3 minutes), ~170 GB peak memory
- Python: ~3,722 seconds (62 minutes), ~652 GB peak memory

## Recommendations

### Immediate Actions

1. **Fix Data Type Bug (CRITICAL)**
   - Update `xarray_dbd/backend.py` to preserve sensor dtypes
   - Add test to verify output file types match C++ version
   - This will reduce output file sizes by ~28x

2. **Optimize Memory Usage**
   - Consider streaming approach for large file sets
   - Reduce peak memory footprint (currently 213x input size)
   - Target: Match C++ ratio (~56x input size)

3. **Add Cache Generation**
   - Implement cache file generation for missing sensor lists
   - Fall back to reading sensor list from file (already partially implemented)

### Future Optimizations

1. **Performance Improvements**
   - Profile Python implementation to find bottlenecks
   - Consider Cython/numba for hot paths
   - Optimize array allocations and copies
   - Target: 2x of C++ speed would be acceptable

2. **Benchmarking Suite**
   - Add automated performance regression tests
   - Track speed and memory over time
   - Compare against C++ baseline

3. **GIL-Free Operation**
   - Test with Python 3.13t (free-threaded)
   - Parallel file processing
   - Could offset speed difference with parallelism

## Conclusion

The Python implementation is functional and provides good integration with the Python scientific ecosystem. However, it has:

1. **Critical Bug**: Data types not preserved → 28x larger output files
2. **Performance Gap**: 3.6x slower, 3.8x more memory
3. **Same Functionality**: Both versions fail on `.ecd` files (missing cache)

**Verdict**: The Python version is usable for small-to-medium datasets after fixing the data type bug. The C++ version remains superior for production batch processing and large-scale operations.

---

## Test Environment

- **System**: macOS (ARM64)
- **Python**: 3.14
- **Input**: Real glider data files from dbd_files/
- **Date**: 2025-11-20
- **Tool**: Custom benchmark script using psutil for memory monitoring
