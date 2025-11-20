# xarray-dbd Examples

Simple, minimalist examples showing how to use xarray-dbd to read glider DBD files.

## Quick Start

### Install xarray-dbd

```bash
pip install xarray-dbd
```

Or for development:

```bash
git clone https://github.com/mousebrains/dbd2netcdf-python.git
cd dbd2netcdf-python
pip install -e .
```

## Examples

### 1. Load a Single File

**File:** `load_single_file.py`

The simplest case - load one DBD file and explore its contents.

```python
import xarray_dbd as xdbd

# Load the file
ds = xdbd.open_dbd_dataset("path/to/file.dbd")

# Print info
print(ds)

# Access a variable
depth = ds["m_depth"]

# Save to NetCDF
ds.to_netcdf("output.nc")
```

**Key features:**
- Direct file loading
- Automatic sensor detection
- Native xarray Dataset output
- Easy conversion to NetCDF or pandas

### 2. Load Multiple Files

**File:** `load_multiple_files.py`

Load and concatenate multiple files using glob patterns.

```python
from pathlib import Path
import xarray_dbd as xdbd

# Find all DBD files
files = sorted(Path("data/").glob("*.dbd"))

# Load and concatenate
ds = xdbd.open_multi_dbd_dataset(files)

print(f"Loaded {len(ds.i)} records from {len(files)} files")
```

**Key features:**
- Automatic file concatenation
- Glob pattern support
- Mission filtering
- Sensor selection

### 3. Advanced Usage

**File:** `advanced_usage.py`

More advanced operations including filtering, statistics, and exports.

```python
import xarray_dbd as xdbd

# Load with filtering
ds = xdbd.open_multi_dbd_dataset(
    files,
    to_keep=["m_depth", "m_lat", "m_lon"],  # Only these sensors
    skip_missions=["initial.mi"],             # Skip test missions
)

# Filter by depth
deep = ds.where(ds["m_depth"] > 10, drop=True)

# Export to different formats
ds.to_netcdf("data.nc")
ds.to_dataframe().to_csv("data.csv")
```

**Key features:**
- Sensor filtering
- Mission filtering
- Data subsetting
- Multiple export formats

## Common Use Cases

### Load Flight Data (.dbd files)

```python
files = sorted(Path("data/").glob("*.dbd"))
ds = xdbd.open_multi_dbd_dataset(files)
```

### Load Science Data (.ebd files)

```python
files = sorted(Path("data/").glob("*.ebd"))
ds = xdbd.open_multi_dbd_dataset(files)
```

### Load Both Flight and Science

```python
# Load separately
flight = xdbd.open_multi_dbd_dataset(sorted(Path("data/").glob("*.dbd")))
science = xdbd.open_multi_dbd_dataset(sorted(Path("data/").glob("*.ebd")))

# Or load all together (they'll be concatenated)
all_files = sorted(Path("data/").glob("*.[de]bd"))
ds = xdbd.open_multi_dbd_dataset(all_files)
```

### Select Specific Sensors

```python
# Only navigation sensors
nav_sensors = ["m_present_time", "m_lat", "m_lon", "m_depth"]
ds = xdbd.open_multi_dbd_dataset(files, to_keep=nav_sensors)

# Only science sensors
sci_sensors = ["sci_water_temp", "sci_water_cond", "sci_water_pressure"]
ds = xdbd.open_multi_dbd_dataset(files, to_keep=sci_sensors)
```

### Skip Test Missions

```python
# Skip common test missions
ds = xdbd.open_multi_dbd_dataset(
    files,
    skip_missions=[
        "initial.mi",
        "status.mi",
        "lastgasp.mi",
        "overtime.mi",
    ]
)
```

### Use with Xarray Operations

Since the output is a standard xarray Dataset, all xarray operations work:

```python
# Select by index
subset = ds.isel(i=slice(0, 100))

# Select by value
shallow = ds.where(ds["m_depth"] < 5, drop=True)

# Compute statistics
mean_temp = ds["sci_water_temp"].mean()
max_depth = ds["m_depth"].max()

# Resample (if you have time as a dimension)
# Note: you may need to convert m_present_time to a proper datetime coordinate
```

## File Types

xarray-dbd supports all standard glider binary file types:

| Extension | Description |
|-----------|-------------|
| `.dbd` | Flight data (full resolution) |
| `.ebd` | Science data (full resolution) |
| `.sbd` | Flight data (decimated, surface) |
| `.tbd` | Science data (decimated, surface) |
| `.mbd` | Flight data (decimated) |
| `.nbd` | Science data (decimated) |

Compressed versions (`.dcd`, `.ecd`, `.scd`, `.tcd`, `.mcd`, `.ncd`) are automatically detected and decompressed.

## Performance Tips

1. **Use caching**: Sensor definitions are cached in a `cache/` directory, speeding up subsequent reads
2. **Filter early**: Use `to_keep` to load only needed sensors
3. **Skip test missions**: Use `skip_missions` to avoid processing test data
4. **Compressed files**: `.?cd` files are automatically decompressed using LZ4

## Getting Help

For more information:
- See the main [README.md](../README.md)
- Check the [CLAUDE.md](../CLAUDE.md) for architecture details
- Visit the [GitHub repository](https://github.com/mousebrains/dbd2netcdf-python)

## Running the Examples

Make sure to update the file paths in each example to point to your actual DBD files:

```python
# Change this:
ds = xdbd.open_dbd_dataset("path/to/your/file.dbd")

# To something like:
ds = xdbd.open_dbd_dataset("/data/glider/01330000.dbd")
```

Then run:

```bash
python examples/load_single_file.py
python examples/load_multiple_files.py
python examples/advanced_usage.py
```
