# xarray-dbd

[![PyPI](https://img.shields.io/pypi/v/xarray-dbd)](https://pypi.org/project/xarray-dbd/)
[![Python](https://img.shields.io/pypi/pyversions/xarray-dbd)](https://pypi.org/project/xarray-dbd/)
[![License](https://img.shields.io/pypi/l/xarray-dbd)](License.txt)
[![CI](https://github.com/mousebrains/dbd2netcdf-python/actions/workflows/ci.yml/badge.svg)](https://github.com/mousebrains/dbd2netcdf-python/actions/workflows/ci.yml)
[![CodeQL](https://github.com/mousebrains/dbd2netcdf-python/actions/workflows/codeql.yml/badge.svg)](https://github.com/mousebrains/dbd2netcdf-python/actions/workflows/codeql.yml)
[![Codecov](https://codecov.io/gh/mousebrains/dbd2netcdf-python/branch/main/graph/badge.svg)](https://codecov.io/gh/mousebrains/dbd2netcdf-python)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

An efficient xarray backend for reading Dinkum Binary Data (DBD) files from
[Slocum ocean gliders](https://www.teledynemarine.com/brands/webb-research/slocum-glider).
Slocum gliders are autonomous underwater vehicles widely used in oceanography to collect
temperature, salinity, and other water-column measurements along sawtooth profiles.

This package provides native xarray support for DBD files, allowing you to read glider data
directly into xarray Datasets without intermediate NetCDF conversion. The C++ binary parser
(via [pybind11](https://pybind11.readthedocs.io/)) matches the performance of the original
[dbd2netCDF](https://github.com/mousebrains/dbd2netcdf) tool.

## Features

- **Native xarray integration**: Read DBD files directly with `xarray.open_dataset()`
- **High performance**: Efficient binary parsing matching dbd2netCDF performance
- **Multiple file support**: Easily concatenate multiple DBD files
- **Flexible filtering**: Select specific sensors and missions
- **Automatic repair**: Optional corrupted data recovery
- **Full metadata**: Preserves sensor units and file attributes

## Installation

**Requires Python 3.10+**

```bash
pip install xarray-dbd
```

For the CLI tools only:

```bash
pipx install xarray-dbd   # installs dbd2nc and mkone commands
```

Or install from source (requires a C++ compiler and CMake):

```bash
git clone https://github.com/mousebrains/dbd2netcdf-python
cd dbd2netcdf-python
pip install -e .
```

## Quick Start

### Reading a single DBD file

```python
import xarray as xr
import xarray_dbd as xdbd

# Method 1: Using xarray's open_dataset with engine parameter
ds = xr.open_dataset('test.sbd', engine='dbd')

# Method 2: Using convenience function
ds = xdbd.open_dbd_dataset('test.sbd')

# Access data
print(ds)
print(ds['m_present_time'])
print(ds['m_depth'])
```

### Reading multiple DBD files

```python
import xarray_dbd as xdbd
from pathlib import Path

# Get all sbd files
files = sorted(Path('.').glob('*.sbd'))

# Read and concatenate
ds = xdbd.open_multi_dbd_dataset(files)

print(f"Total records: {len(ds.i)}")
print(f"Variables: {list(ds.data_vars)}")
```

### Filtering sensors

```python
# Only keep specific sensors
ds = xdbd.open_dbd_dataset(
    'test.sbd',
    to_keep=['m_present_time', 'm_depth', 'm_lat', 'm_lon']
)
```

### Filtering missions

```python
# Skip certain missions
ds = xdbd.open_multi_dbd_dataset(
    files,
    skip_missions=['initial.mi', 'status.mi']
)

# Or keep only specific missions
ds = xdbd.open_multi_dbd_dataset(
    files,
    keep_missions=['mission1.mi', 'mission2.mi']
)
```

### Advanced options

```python
ds = xdbd.open_dbd_dataset(
    'test.sbd',
    skip_first_record=True,  # Skip first record (default)
    repair=True,             # Attempt to repair corrupted data
    to_keep=['m_*'],         # Keep sensors matching pattern (future feature)
    criteria=['m_present_time'],  # Sensors for record selection
)
```

## DBD File Format

DBD (Dinkum Binary Data) files are the native format used by Slocum ocean gliders. The format consists of:

1. **ASCII Header**: Mission metadata and configuration
2. **Sensor List**: Definitions of all sensors with names, units, and data types
3. **Known Bytes**: Endianness detection section
4. **Compressed Data**: Efficiently encoded sensor readings using:
   - Run-length encoding for unchanged values
   - Variable-length records with 2-bit codes per sensor
   - Support for 1, 2, 4, and 8-byte sensor values

## Performance

See [docs/performance.md](docs/performance.md) for benchmarks, memory
analysis, and methodology.

## API Reference

### `open_dbd_dataset(filename, **kwargs)`

Open a single DBD file as an xarray Dataset.

**Parameters:**
- `filename` (str or Path): Path to DBD file
- `skip_first_record` (bool): Skip first data record (default: True)
- `repair` (bool): Attempt to repair corrupted records (default: False)
- `to_keep` (list of str): Sensor names to keep (default: all)
- `criteria` (list of str): Sensor names for selection criteria
- `drop_variables` (list of str): Variables to exclude

**Returns:** `xarray.Dataset`

### `open_multi_dbd_dataset(filenames, **kwargs)`

Open multiple DBD files as a single concatenated xarray Dataset.

**Parameters:**
- `filenames` (iterable): Paths to DBD files
- `skip_first_record` (bool): Skip first record in each file (default: True)
- `repair` (bool): Attempt to repair corrupted records (default: False)
- `to_keep` (list of str): Sensor names to keep (default: all)
- `criteria` (list of str): Sensor names for selection criteria
- `skip_missions` (list of str): Mission names to skip
- `keep_missions` (list of str): Mission names to keep

**Returns:** `xarray.Dataset`

## Migration from dbdreader

The dbdreader2 API is derived from Lucas Merckelbach's
[dbdreader](https://github.com/smerckel/dbdreader) library. xarray-dbd
provides drop-in `DBD` and `MultiDBD` classes that mirror the dbdreader API.
For a fully transparent swap, alias the import:

```python
# Before (dbdreader)
import dbdreader
dbd = dbdreader.DBD("file.dcd", cacheDir="cache")
t, depth = dbd.get("m_depth")

mdbd = dbdreader.MultiDBD(filenames=files, cacheDir="cache")
t, temp, sal = mdbd.get_sync("sci_water_temp", "sci_water_cond")

# After (xarray-dbd) — same API
import xarray_dbd.dbdreader2 as dbdreader   # drop-in replacement
dbd = dbdreader.DBD("file.dcd", cacheDir="cache")
t, depth = dbd.get("m_depth")

mdbd = dbdreader.MultiDBD(filenames=files, cacheDir="cache")
t, temp, sal = mdbd.get_sync("sci_water_temp", "sci_water_cond")
```

The top-level `xarray_dbd` namespace also re-exports `DBD` and `MultiDBD`
for convenience:

```python
import xarray_dbd as xdbd
dbd = xdbd.DBD("file.dcd", cacheDir="cache")
```

| Feature | xarray-dbd dbdreader2 | dbdreader |
|---------|----------------------|-----------|
| `get(*params)` | Yes | Yes |
| `get_sync(*params)` | Yes (`np.interp`) | Yes (C ext) |
| `parameterNames` | Yes | Yes |
| `parameterUnits` | Yes | Yes |
| `has_parameter()` | Yes | Yes |
| `get_xy()`, `get_CTD_sync()` | Yes | Yes |
| `decimalLatLon` | Yes | Yes |
| `set_time_limits()` | Yes | Yes |
| `include_source` | Yes | Yes |

### Use-case examples

**Single file — `get()` one or more parameters:**

```python
import xarray_dbd.dbdreader2 as dbdreader

dbd = dbdreader.DBD("unit_123-2024-100-0-0.dcd", cacheDir="cache")

# Single parameter → (time, values)
t, depth = dbd.get("m_depth")

# Multiple parameters → list of (time, values) tuples
results = dbd.get("m_depth", "m_pitch", "m_roll")
for t, v in results:
    print(t.shape, v.shape)

dbd.close()
```

**Synchronized reads — `get_sync()` and `get_xy()`:**

```python
# get_sync: all values interpolated onto the first parameter's time base
t, depth, pitch = dbd.get_sync("m_depth", "m_pitch")

# get_xy: y interpolated onto x's time base (returns x, y arrays)
depth_vals, pitch_vals = dbd.get_xy("m_depth", "m_pitch")
```

**Multi-file — `MultiDBD`:**

```python
# Explicit file list
mdbd = dbdreader.MultiDBD(filenames=["a.dcd", "b.dcd"], cacheDir="cache")

# Or glob pattern
mdbd = dbdreader.MultiDBD(pattern="/data/glider/*.dcd", cacheDir="cache")

t, depth = mdbd.get("m_depth")
print(f"{len(t)} records across {len(mdbd.filenames)} files")
mdbd.close()
```

**CTD synchronization — `get_CTD_sync()`:**

```python
mdbd = dbdreader.MultiDBD(filenames=ebd_files, cacheDir="cache")
tctd, C, T, P = mdbd.get_CTD_sync()
# Or with extra parameters synced to the CTD time base:
tctd, C, T, P, depth = mdbd.get_CTD_sync("m_depth")
```

**Time limits:**

```python
mdbd = dbdreader.MultiDBD(pattern="*.dcd", cacheDir="cache")
print(mdbd.get_time_range())            # ['01 Jan 2024 00:00', '15 Jan 2024 23:59']

mdbd.set_time_limits("5 Jan", "10 Jan")  # filter by file open time
t, depth = mdbd.get("m_depth")           # only data from 5–10 Jan
```

**Mission filtering:**

```python
# Exclude specific missions
mdbd = dbdreader.MultiDBD(
    pattern="*.dcd", cacheDir="cache",
    banned_missions=["initial.mi", "status.mi"],
)

# Or include only specific missions
mdbd = dbdreader.MultiDBD(
    pattern="*.dcd", cacheDir="cache",
    missions=["science_survey.mi"],
)
print(mdbd.mission_list)  # unique mission names (sorted)
```

**Complement files — automatic eng/sci pairing:**

```python
# Pair each .dcd with its .ecd counterpart (or vice versa)
mdbd = dbdreader.MultiDBD(
    pattern="/data/*.dcd", cacheDir="cache",
    complement_files=True,
)
# mdbd.parameterNames["eng"] + mdbd.parameterNames["sci"] both populated
```

### Key differences from dbdreader

- **Lazy incremental loading.** Construction only scans file headers and
  sensor metadata — no data records are read. Each `get()` call loads
  only the newly-requested columns (plus the time variable) and caches
  them for future calls. This keeps peak RSS proportional to the sensors
  you actually use, not the total sensor count. Pass `preload=["s1", "s2"]`
  to batch additional sensors into the first `get()` call.

- **`skip_initial_line` semantics.** When reading multiple files, the
  first contributing file keeps all its records; subsequent files skip
  their first record. dbdreader skips the first record of every file.
  Multi-file record counts may therefore differ by up to N-1.

- **Float64 output.** `get()` always returns float64 arrays, matching
  dbdreader's behavior. Integer fill values (-127 for int8, -32768 for
  int16) are filtered out (with `return_nans=False`) or replaced with
  NaN (with `return_nans=True`).

- **Time limits are per-file.** `set_time_limits()` filters by file
  open time, including or excluding entire files. It does not filter
  individual records within a file. dbdreader also filters by file open
  time, so this is operationally the same for most use cases.

- **Error handling.** The same `DbdError` exception class and numeric
  error codes (`DBD_ERROR_CACHE_NOT_FOUND`, etc.) are provided for
  compatibility.

### dbdreader2 API reference

#### `DBD` — single file reader

```python
DBD(filename, cacheDir=None, skip_initial_line=True, preload=None)
```

| Property | Type | Description |
|----------|------|-------------|
| `parameterNames` | `list[str]` | Available sensor names |
| `parameterUnits` | `dict[str, str]` | `{sensor: unit}` mapping |
| `timeVariable` | `str` | `"m_present_time"` or `"sci_m_present_time"` |
| `filename` | `str` | Path to the opened file |
| `headerInfo` | `dict` | Header key-value pairs |

| Method | Returns | Description |
|--------|---------|-------------|
| `get(*params, decimalLatLon=True, return_nans=False)` | `(t, v)` or `[(t, v), ...]` | Extract parameter data |
| `get_sync(*params)` | `(t, v0, v1, ...)` | Interpolated to first param's time |
| `get_xy(param_x, param_y)` | `(x, y)` | y interpolated onto x's time |
| `has_parameter(name)` | `bool` | Check sensor availability |
| `get_mission_name()` | `str` | Mission name (lowercase) |
| `get_fileopen_time()` | `float` | File open time (epoch seconds) |
| `close()` | — | Release stored data |

#### `MultiDBD` — multi-file reader

```python
MultiDBD(
    filenames=None, pattern=None, cacheDir=None,
    complement_files=False, complemented_files_only=False,
    banned_missions=(), missions=(),
    max_files=None, skip_initial_line=True, preload=None,
)
```

| Property | Type | Description |
|----------|------|-------------|
| `parameterNames` | `dict[str, list]` | `{"eng": [...], "sci": [...]}` |
| `parameterUnits` | `dict[str, str]` | Union of eng + sci units |
| `filenames` | `list[str]` | All loaded file paths |
| `mission_list` | `list[str]` | Unique mission names (sorted) |
| `time_limits_dataset` | `tuple` | `(min_time, max_time)` for full dataset |

| Method | Returns | Description |
|--------|---------|-------------|
| `get(*params, decimalLatLon=True, return_nans=False)` | `(t, v)` or `[(t, v), ...]` | Extract from combined eng+sci data |
| `get_sync(*params, interpolating_function_factory=None)` | `(t, v0, v1, ...)` | Synced to first param's time |
| `get_xy(x, y, interpolating_function_factory=None)` | `(x, y)` | y interpolated onto x's time |
| `get_CTD_sync(*extra, interpolating_function_factory=None)` | `(t, C, T, P, ...)` | CTD-synced data with quality filters |
| `has_parameter(name)` | `bool` | Check sensor availability |
| `set_time_limits(minTimeUTC=None, maxTimeUTC=None)` | — | Filter by file open time; triggers reload |
| `get_time_range(fmt=...)` | `[start, end]` | Formatted time range of current selection |
| `get_global_time_range(fmt=...)` | `[start, end]` | Formatted time range of entire dataset |
| `close()` | — | Release all data |

## Comparison with dbd2netCDF

| Feature | xarray-dbd | dbd2netCDF |
|---------|------------|------------|
| Language | C++ via pybind11 | C++ |
| xarray integration | Native | Via NetCDF |
| Installation | `pip install` | Compile from source |
| Dependencies | numpy, xarray | NetCDF, HDF5 libraries |
| Performance | Comparable | Fast |
| Multi-file | Built-in | Manual |

## Examples

See [examples/Examples.md](examples/Examples.md) for standalone scripts with
plots and detailed documentation.

### Basic data exploration

```python
import xarray_dbd as xdbd

ds = xdbd.open_dbd_dataset('test.sbd')

# Print dataset info
print(ds)

# Get data dimensions
print(f"Number of records: {len(ds.i)}")

# List all variables
print("Variables:", list(ds.data_vars))

# Access sensor data
depth = ds['m_depth'].values
time = ds['m_present_time'].values

# Get attributes
print(f"Mission: {ds.attrs['mission_name']}")
print(f"Depth units: {ds['m_depth'].attrs['units']}")
```

### Working with trajectories

```python
import xarray_dbd as xdbd
import matplotlib.pyplot as plt

# Read flight data
files = sorted(Path('.').glob('*.sbd'))
ds = xdbd.open_multi_dbd_dataset(files)

# Plot depth vs time
plt.figure(figsize=(12, 4))
plt.plot(ds['m_present_time'], ds['m_depth'])
plt.gca().invert_yaxis()
plt.xlabel('Time')
plt.ylabel('Depth (m)')
plt.title(f"Mission: {ds.attrs.get('mission_name', 'Unknown')}")
plt.show()
```

### Extracting science data

```python
# Read full resolution science data
files = sorted(Path('.').glob('*.ebd'))
ds = xdbd.open_multi_dbd_dataset(
    files,
    to_keep=['m_present_time', 'sci_water_temp', 'sci_water_cond']
)

# Convert to pandas for analysis
df = ds.to_dataframe()
print(df.describe())
```

## Known Limitations

- **Python 3.10+ required** — uses `from __future__ import annotations` for modern type-hint syntax.
- **Free-threaded Python (3.13t)** — pybind11 extensions may crash under the
  no-GIL build; this is an upstream pybind11 limitation.
- **Timestamps are raw floats** — `m_present_time` values are Unix epoch
  seconds (float64). Convert with `pandas.to_datetime(ds['m_present_time'], unit='s')`.
- **No lazy loading for xarray API** — `open_dataset()` reads all sensor data
  into memory. For very large deployments, use `to_keep` to select only needed
  sensors. The dbdreader2 API (`DBD`/`MultiDBD`) uses lazy incremental loading.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `ImportError: _dbd_cpp` | Reinstall with `pip install -e .` — the C++ extension needs compiling. |
| `RuntimeError: ... cache ...` | Pass `cache_dir=` pointing to the directory containing `.cac`/`.ccc` files. |
| Empty dataset (0 records) | Check that the file isn't a header-only stub (0 data records). |
| `OSError: Failed to read` | The file may be truncpted or use an unsupported format version. Try `repair=True`. |

## Development

### Running tests

```bash
pip install -e ".[dev]"
pytest
```

### Code formatting

```bash
ruff format xarray_dbd/
ruff check xarray_dbd/
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for full development setup instructions.

## License

This project is based on [dbd2netCDF](https://github.com/mousebrains/dbd2netcdf)
by Pat Welch and is licensed under the GNU General Public License v3.0.

## Credits

- Original dbd2netCDF implementation: Pat Welch (pat@mousebrains.com)
- DBD format documentation: The Slocum glider community
- xarray backend interface: xarray developers

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## Citation

If you use this software in your research, please cite both this package and
the original dbd2netCDF tool.
