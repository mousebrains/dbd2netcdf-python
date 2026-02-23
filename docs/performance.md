# Performance Analysis

Detailed benchmarks comparing **xarray-dbd** against
[dbdreader](https://pypi.org/project/dbdreader/) for reading Slocum
glider Dinkum Binary Data (DBD) files.

## Test Setup

- **Dataset**: 18 compressed `.dcd` files, 18,054 total records, 1,706 sensors
- **Hardware**: Apple M4 Max
- **OS**: macOS 26 (Tahoe)
- **Python**: 3.14
- **xarray-dbd**: 0.2.1 (C++ parser via pybind11)
- **dbdreader**: 0.5.8 (C extension, CPython API)

## Results

Best of 3 runs per scenario. dbdreader is tested two ways:
- **per sensor**: calls `get()` once per variable in a loop (re-parses the file each time)
- **all at once**: calls `get(*params)` with all variables unpacked (single file parse per call)

### Wall Time

| Scenario | xarray-dbd | dbdreader (per sensor) | dbdreader (all at once) |
|---|--:|--:|--:|
| Single file, all sensors | **530 ms** | 560 ms | 470 ms |
| Single file, 5 sensors | 460 ms | **410 ms** | 410 ms |
| 18 files, all sensors | **620 ms** | 205 s | 8.4 s |
| 18 files, 5 sensors | **600 ms** | 880 ms | 560 ms |

### Peak RSS (above baseline)

Peak resident set size measured via `/usr/bin/time -l` in isolated
subprocesses, minus the baseline RSS of `python -c ''` (~16 MB).
Captures all memory including Python heap, C/C++ allocations, and
shared libraries.

| Scenario | xarray-dbd | dbdreader (per sensor) | dbdreader (all at once) |
|---|--:|--:|--:|
| Single file, all sensors | 120 MB | **59 MB** | 87 MB |
| Single file, 5 sensors | 117 MB | **59 MB** | 59 MB |
| 18 files, all sensors | 992 MB | **71 MB** | 323 MB |
| 18 files, 5 sensors | 90 MB | **69 MB** | 72 MB |

### Analysis

**Wall time.** Single-file timings are dominated by Python startup and
import overhead (~0.5 s), making the libraries comparable. The real
difference shows in multi-file scenarios: xarray-dbd reads all sensors
in a single pass per file, so cost is proportional to data volume
regardless of sensor count. dbdreader's `get()` re-opens and re-parses
the binary file on every call — the per-sensor loop calls `get()` 1,705
times across 18 files (205 s), while the batch approach re-parses each
file only once (8.4 s). xarray-dbd is ~14x faster than even the batch
approach.

**Memory.** xarray-dbd uses more memory because the C++ backend
materializes all sensor columns simultaneously. For the 18-file,
all-sensor case, xarray-dbd peaks at 992 MB (18,054 records x 1,706
sensors in typed arrays). dbdreader's per-sensor approach stays at
~70 MB because each `get()` returns one sensor's data and Python can
collect intermediates. The batch approach peaks at 323 MB since all
sensor arrays must coexist in memory.

For filtered reads (5 sensors), xarray-dbd memory is modest (90 MB)
because the C++ backend discards non-matching columns early.

## NetCDF Writer: C++ dbd2netCDF vs xdbd 2nc

End-to-end comparison of writing 18 `.dcd` files to a compressed NetCDF
file. The C++ standalone (`dbd2netCDF`) writes directly; `xdbd 2nc` uses
the streaming writer (`write_multi_dbd_netcdf`).

| Metric | C++ dbd2netCDF | xdbd 2nc |
|---|--:|--:|
| Wall time | **610 ms** | 1.85 s |
| Peak RSS | **305 MB** | 1,240 MB |
| Output size | 9.1 MB | 7.5 MB |

`xdbd 2nc` is ~3x slower and uses ~4x more memory than the C++ standalone.
The Python overhead comes from subprocess startup, import time, and
holding all sensor columns in memory during the write. The smaller output
from `xdbd 2nc` is due to different default chunking parameters.

## Methodology

- **Wall time**: `/usr/bin/time -l` real time, best of 3 isolated
  subprocess runs. Includes Python startup and import overhead.
- **Peak RSS**: `maximum resident set size` from `/usr/bin/time -l`,
  minus baseline (bare `python -c ''`). Captures both Python and
  C/C++ heap allocations.
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
