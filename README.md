# xarray-dbd

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

**Requires Python 3.13+**

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

The xarray-dbd engine is designed for efficiency:

- **Direct binary parsing**: No intermediate file conversion
- **Efficient compression handling**: Native support for DBD's run-length encoding
- **Memory efficient**: Lazy loading where possible
- **Fast concatenation**: Optimized multi-file reading

Performance is comparable to or better than dbd2netCDF for most use cases.

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

- **Python 3.13+ required** — uses modern type-hint syntax and C API features.
- **Free-threaded Python (3.13t)** — pybind11 extensions may crash under the
  no-GIL build; this is an upstream pybind11 limitation.
- **Timestamps are raw floats** — `m_present_time` values are Unix epoch
  seconds (float64). Convert with `pandas.to_datetime(ds['m_present_time'], unit='s')`.
- **No lazy loading** — all sensor data is read into memory on `open_dataset()`.
  For very large deployments, use `to_keep` to select only needed sensors.

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
