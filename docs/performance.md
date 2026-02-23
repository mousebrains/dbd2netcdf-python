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

Best of 3 runs per scenario. dbdreader is tested two ways:
- **per sensor**: calls `get()` once per variable in a loop (re-parses the file each time)
- **all at once**: calls `get(*params)` with all variables unpacked (single file parse per call)

| Scenario | xarray-dbd | dbdreader (per sensor) | dbdreader (all at once) |
|---|--:|--:|--:|
| Single file, all sensors | **47 ms** | 187 ms | 383 ms |
| Single file, 5 sensors | 6 ms | 7 ms | **6 ms** |
| 18 files, all sensors | **105 ms** | 208 s | 20.1 s |
| 18 files, 5 sensors | **32 ms** | 705 ms | 364 ms |

### Analysis

xarray-dbd reads **all sensors in a single pass** per file. Every
data record is decoded once and each sensor value is stored into a
pre-allocated typed column. This makes whole-dataset access very fast
regardless of sensor count.

dbdreader's `get()` accepts multiple sensor names and extracts them
all in a single file parse via its C extension. However, there is no
data caching — each call to `get()` re-opens and re-parses the binary
file from scratch. For the per-sensor loop, cost scales as
**N_sensors x N_files**. For the 18-file, all-sensor scenario, the
per-sensor approach calls `get()` 1,705 times across 18 files (208 s),
while the batch approach re-parses each file only once (20.1 s).
xarray-dbd is still ~190x faster than even the batch approach.

For the single-file all-sensor case, dbdreader's batch `get()` is
actually slower than the per-sensor loop (383 ms vs 187 ms), likely
due to overhead of returning all sensor arrays at once.

For extracting a small number of sensors from a single file, all three
approaches are comparable (~6-7 ms). xarray-dbd always decodes all
sensors (even when `to_keep` is set) to maintain binary stream
alignment, then discards the unwanted columns.

## Peak Memory (Python heap via tracemalloc)

Peak Python-level allocations measured via `tracemalloc`. Note that
xarray-dbd's C++ backend allocates column arrays in C++ which
tracemalloc does not track, so xarray-dbd numbers undercount true
memory usage. dbdreader's C extension returns numpy arrays to Python,
so its allocations are mostly visible to tracemalloc.

| Scenario | xarray-dbd | dbdreader (per sensor) | dbdreader (all at once) |
|---|--:|--:|--:|
| Single file, all sensors | 2.6 MB | 1.1 MB | 1.6 MB |
| Single file, 5 sensors | 262 KB | 670 KB | 669 KB |
| 18 files, all sensors | 2.4 MB | 83.9 MB | 169.2 MB |
| 18 files, 5 sensors | 19 KB | 9.4 MB | 9.7 MB |

### Analysis

For the multi-file all-sensor scenario, dbdreader's batch `get()` peaks
at 169 MB — roughly double the per-sensor approach (84 MB) — because it
must return all sensor arrays simultaneously rather than one at a time
where Python can collect intermediate results.

xarray-dbd's tracemalloc numbers appear low because the C++ backend
allocates typed column arrays outside the Python heap. The returned
xarray Dataset wraps these arrays without copying. Actual process RSS
is significantly higher (see Large Deployment below).

## Methodology

- **Wall time**: `time.perf_counter()`, best of 3 runs.
- **Peak memory**: `tracemalloc` peak, max across 3 runs. Tracks
  Python-level allocations only — C/C++ heap allocations (e.g.
  xarray-dbd's column arrays) are not captured.
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
