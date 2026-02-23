# Performance Analysis

Detailed benchmarks comparing **xarray-dbd** against
[dbdreader](https://pypi.org/project/dbdreader/) for reading Slocum
glider Dinkum Binary Data (DBD) files.

## Test Setup

- **Dataset**: 18 compressed `.dcd` files, 18,054 total records, 1,706 sensors
- **Hardware**: Apple M2 Pro, 16 GB RAM
- **OS**: macOS 15 (Sequoia)
- **Python**: 3.12
- **xarray-dbd**: 0.1.0 (C++ parser via pybind11)
- **dbdreader**: 0.5.4 (C extension, CPython API)

## Wall Time

Best of 3 runs per scenario.

| Scenario | xarray-dbd | dbdreader | |
|---|--:|--:|---|
| Single file, all sensors | **19 ms** | 149 ms | xarray-dbd 8x faster |
| Single file, 5 sensors | 5 ms | **2 ms** | dbdreader 2.5x faster |
| 18 files, all sensors | **113 ms** | 212 s | xarray-dbd 1,900x faster |
| 18 files, 5 sensors | **34 ms** | 502 ms | xarray-dbd 15x faster |

### Analysis

xarray-dbd reads **all sensors in a single pass** per file. Every
data record is decoded once and each sensor value is stored into a
pre-allocated typed column. This makes whole-dataset access very fast
regardless of sensor count.

dbdreader's `get()` re-reads the file for each requested sensor, using
`fseek()` to skip bytes for sensors not being read. This means cost
scales as **N_sensors x N_files**. For the 18-file, all-sensor scenario,
dbdreader calls `get()` 1,705 times across 18 files — hence the ~212 s
wall time vs. xarray-dbd's 113 ms.

For extracting a small number of sensors from a single file, dbdreader
is faster because it seeks past unneeded bytes rather than decoding
them. xarray-dbd always decodes all sensors (even when `to_keep` is
set) to maintain binary stream alignment, then discards the unwanted
columns.

## Memory (RSS)

Resident Set Size measured via `resource.getrusage(RUSAGE_SELF).ru_maxrss`
in isolated subprocesses (one measurement per scenario).

| Scenario | xarray-dbd | dbdreader |
|---|--:|--:|
| Import only | 84 MB | 59 MB |
| Single file, all sensors | 118 MB | 58 MB |
| Single file, 5 sensors | 116 MB | 58 MB |
| 18 files, all sensors | 990 MB | 147 MB |
| 18 files, 5 sensors | 87 MB | 69 MB |

### Analysis

xarray-dbd has higher baseline RSS because it imports xarray + numpy +
pybind11 (84 MB) vs. dbdreader's numpy + C extension (59 MB).

For the multi-file all-sensors scenario, xarray-dbd peaks at ~990 MB
because the C++ two-pass reader holds all 18,054 records x 1,706 sensors
in typed column arrays simultaneously. dbdreader's 147 MB reflects
reading one sensor at a time — each `get()` returns a small array that
Python can collect before the next call.

For filtered reads (5 sensors), xarray-dbd memory is modest (87 MB)
because the C++ backend discards non-matching columns early.

## Methodology

- **Wall time**: `time.perf_counter()`, best of 3 runs, measured after
  a warm-up call.
- **RSS memory**: `resource.getrusage(RUSAGE_SELF).ru_maxrss` in fresh
  subprocess invocations. This captures both Python and C/C++ heap
  allocations. Note that Python's `tracemalloc` only tracks Python-level
  allocations and misses C/C++ memory entirely.
- **Benchmark script**: `scripts/benchmark_comparison.py`

## Large Deployment

On a production dataset (908 files, 1.26 M records, 1,706 sensors):

- **dbdreader**: Fails with a cache-parsing error and cannot process the
  dataset.
- **xarray-dbd**: Processes the full dataset in ~7 s (streaming writer)
  with ~8.7 GB peak RSS.

The streaming writer (`write_multi_dbd_netcdf`) processes one batch of
files at a time, keeping memory proportional to the batch size rather
than the full dataset.
