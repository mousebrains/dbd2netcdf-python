# xarray-dbd Implementation Status

## Summary

I've created an xarray backend engine for reading Dinkum Binary Data (DBD) files from ocean gliders. The implementation is in pure Python and designed to match or exceed the performance of the original dbd2netCDF C++ tool.

## What's Been Completed

### ✅ Core Implementation

1. **Package Structure** (`xarray_dbd/`)
   - `__init__.py` - Package initialization with exports
   - `header.py` - DBD file header parsing
   - `sensor.py` - Sensor metadata classes
   - `reader.py` - Binary data reader with compression support
   - `backend.py` - Xarray backend entrypoint and data store

2. **Features Implemented**
   - Native xarray integration via BackendEntrypoint
   - Efficient binary parsing of DBD format
   - Endianness detection via "known bytes" validation
   - Run-length encoding decompression (codes 0, 1, 2)
   - Multi-file concatenation support
   - Sensor filtering
   - Mission filtering
   - Lazy loading support
   - Full metadata preservation

3. **Package Files**
   - `setup.py` - Standard setuptools configuration
   - `pyproject.toml` - Modern Python packaging
   - `README.md` - Comprehensive documentation with examples
   - Test scripts for validation

## Current Status

### 🔧 Known Issue

There's a parsing discrepancy with the test DBD file (`dbd2netcdf/test/test.sbd`):
- **Expected**: 94 data records (as produced by original dbd2netCDF)
- **Current**: Reading stops after 1 record due to unexpected byte sequence

**Investigation findings**:
- Header parsing: ✅ Correct
- Sensor list parsing: ✅ Correct
- Known bytes (endianness): ✅ Correct
- First record parsing: ✅ Reads 364 sensor values correctly
- **Issue**: After reading 1299 bytes of sensor data, encountering unexpected byte 0xf5 instead of next 'd' tag
- Missing approximately 13 bytes before next record

**Possible causes being investigated**:
1. Special handling for `sensors_per_cycle` field (test file has 11)
2. Additional metadata bytes between records
3. Special encoding for "factored" vs "unfactored" sensor lists
4. Padding or alignment bytes

### ✅ What Works

- File structure detection and validation
- Header parsing with all metadata fields
- Sensor list parsing (all 1804 sensors detected)
- Endianness detection (correctly identifies big-endian test file)
- Bit-level header decoding (2-bit codes per sensor)
- Sensor value reading (all types: int8, int16, float32, float64)
- Sensor filtering by name
- XArray Dataset creation with proper attributes

## Installation

```bash
cd /Users/pat/tpw
pip install -e .
```

## Usage Examples

### Basic Usage

```python
import xarray_dbd as xdbd

# Open a single DBD file
ds = xdbd.open_dbd_dataset('file.sbd')

# Open multiple files
from pathlib import Path
files = sorted(Path('.').glob('*.sbd'))
ds = xdbd.open_multi_dbd_dataset(files)

# Filter sensors
ds = xdbd.open_dbd_dataset(
    'file.sbd',
    to_keep=['m_depth', 'm_lat', 'm_lon', 'm_present_time']
)

# Filter missions
ds = xdbd.open_multi_dbd_dataset(
    files,
    skip_missions=['initial.mi', 'status.mi']
)
```

### Using xarray's open_dataset

```python
import xarray as xr

# The backend auto-registers via entry points
ds = xr.open_dataset('file.sbd', engine='dbd')
```

## Architecture

### DBD File Format (as understood)

```
+-------------------+
| ASCII Header      |  Key-value pairs (mission info, etc.)
+-------------------+
| Sensor List       |  Lines starting with "s:" defining each sensor
+-------------------+
| Known Bytes       |  16 bytes: 's' 'a' 0x1234 123.456f 123456789.12345d
+-------------------+
| Data Records      |  Compressed binary records
|  - 'd' tag (1 byte)
|  - Header bits (n_sensors+3)//4 bytes)
|    - 2 bits per sensor:
|      0 = no data
|      1 = repeat previous value
|      2 = new value (read from stream)
|      3 = (undefined/unused)
|  - New values (variable length)
+-------------------+
| 'X' end tag       |
+-------------------+
```

### Class Hierarchy

```
DBDBackendEntrypoint (xarray backend)
  └─> DBDDataStore
       └─> DBDReader
            ├─> DBDHeader (header parsing)
            ├─> DBDSensors (sensor collection)
            │    └─> DBDSensor (individual sensor)
            └─> KnownBytes (endianness detection)
```

## Next Steps

To complete the implementation, need to:

1. **Debug the 13-byte discrepancy**
   - Compare byte-by-byte with C++ implementation
   - Check for special cases in factored sensor lists
   - Investigate `sensors_per_cycle` handling
   - Look for padding or alignment requirements

2. **Test with real glider data**
   - The test file might be synthetic/minimal
   - Real-world files may work correctly

3. **Performance benchmarking**
   - Compare read speed vs dbd2netCDF
   - Optimize hot paths if needed
   - Profile memory usage

4. **Add comprehensive tests**
   - Unit tests for each component
   - Integration tests with various file types
   - Edge case handling

## Performance Considerations

The implementation uses several optimizations:
- Pre-allocated numpy arrays with estimated size
- Efficient bit manipulation for header codes
- Direct struct unpacking for binary values
- Lazy loading where possible
- Minimal memory copying

Expected performance should be comparable to dbd2netCDF for most operations.

## Files Created

```
/Users/pat/tpw/
├── xarray_dbd/
│   ├── __init__.py
│   ├── header.py
│   ├── sensor.py
│   ├── reader.py
│   └── backend.py
├── setup.py
├── pyproject.toml
├── README.md
├── test_xarray_dbd.py
├── test_with_available.py
└── debug_*.py  (debugging scripts)
```

## Dependencies

- **Required**: `numpy>=1.20`, `xarray>=2022.3.0`
- **Optional**: `pytest`, `black`, `flake8` (for development)

## License

GPL-3.0 (matching original dbd2netCDF)

## Credits

Based on dbd2netCDF by Pat Welch (pat@mousebrains.com)
- Original C++ implementation: https://github.com/mousebrains/dbd2netcdf
- DBD format documentation from Slocum glider community
