# Synchronization Guide: C++ to Python

This document maps the C++ dbd2netCDF implementation to the Python xarray-dbd implementation and provides a process for incorporating future changes.

## File Mapping

### Core Data Reading

| C++ File | Python Module | Purpose | Status |
|----------|---------------|---------|--------|
| `Decompress.C/H` | `xarray_dbd/decompression.py` | LZ4 decompression for TWR format | ✓ Complete |
| `Header.C/H` | `xarray_dbd/header.py` | Parse ASCII header section | ✓ Complete |
| `Sensor.C/H` | `xarray_dbd/sensor.py` | Individual sensor metadata | ✓ Complete |
| `Sensors.C/H` | `xarray_dbd/sensor.py` | Sensor collection management | ✓ Complete |
| `KnownBytes.C/H` | `xarray_dbd/reader.py:KnownBytes` | Endianness detection | ✓ Complete |
| `Data.C/H` | `xarray_dbd/reader.py:DBDReader` | Binary data parsing | ✓ Complete |

### Not Implemented (NetCDF-specific)

| C++ File | Python Equivalent | Notes |
|----------|-------------------|-------|
| `MyNetCDF.C/H` | Native xarray | Python uses xarray's NetCDF writer |
| `DataColumn.C/H` | Not needed | Python uses numpy arrays directly |
| `Variables.C/H` | `xarray_dbd/backend.py` | Handled by xarray Variable objects |

### Not Needed for Core Functionality

| C++ File | Notes |
|----------|-------|
| `SensorsMap.C/H` | Cache management - simplified in Python |
| `FileInfo.C/H` | File utilities - Python pathlib sufficient |
| `StackDump.C/H` | Debug utilities - Python traceback sufficient |
| `Tokenize.C/H` | String parsing - Python str methods sufficient |
| `MyException.H` | Exceptions - Python exceptions sufficient |
| `PD0.C/H`, `pd02netCDF.C` | PD0 ADCP format - out of scope |
| `SGMerge.C/H`, `sgMergeNetCDF.C` | Seaglider format - out of scope |

## Critical Implementation Details

### 1. Decompression (Decompress.C → decompression.py)

**C++ Implementation:**
```cpp
const size_t n(((sz[0] << 8) & 0xff00) | (sz[1] & 0xff));  // Big endian
const size_t j(LZ4_decompress_safe(frame.data(), this->mBuffer, n, sizeof(this->mBuffer)));
```

**Python Equivalent:**
```python
frame_size = struct.unpack('>H', size_bytes)[0]  # Big endian
decompressed = lz4.block.decompress(compressed_data, uncompressed_size=65536)
```

**Key Points:**
- Frame size is 2-byte big-endian uint16
- Buffer size is 65536 bytes (must specify for lz4.block.decompress)

### 2. Sensor Loading (Sensors.C → sensor.py)

**Critical C++ Code:**
```cpp
for (std::string line; getline(is, line);) {
    const Sensor sensor(line);
    if (sensor.qAvailable()) {  // CRITICAL: Only add available sensors
        mSensors.push_back(sensor);
    }
}
```

**Python Equivalent:**
```python
if sensor.available:  # CRITICAL: Only add available sensors
    sensors.add(sensor)
```

**Key Points:**
- **MUST** filter to available sensors (flag 'T' in cache files)
- Not filtering causes sensor index misalignment and wrong data

### 3. Data Reading (Data.C → reader.py)

**C++ Tag Handling:**
```cpp
if (tag != 'd') {
    // Search for next 'd' tag
    while (true) {
        if (!is.read(reinterpret_cast<char*>(&c), 1)) break;
        if (c == 'd') {
            qContinue = true;
            break;
        }
    }
}
```

**Python Equivalent:**
```python
if tag != ord('d'):
    found = False
    while True:
        byte = fp.read(1)
        if not byte:
            break
        if byte[0] == ord('d'):
            tag = ord('d')
            found = True
            break
```

**Key Points:**
- NULL bytes between records are normal (padding)
- Don't stop on 'X' (0x58) during search - it appears in data values
- Only check 'X' as end tag at expected tag position

### 4. Value Reading (Sensor.C → sensor.py)

**C++ Code for Reading:**
```cpp
// For code == 2 (new value)
const double value(sensor.read(is, kb));
if (sensor.qKeep()) {
    row[index] = value;
    prevValue[index] = value;
}
```

**Python Equivalent:**
```python
if code == 2:  # New value
    value = sensor.read_value(fp, self.known_bytes.flip_bytes)
    if sensor.criteria:
        has_criteria = True
    if sensor.keep and sensor.output_index is not None:
        data[n_records, sensor.output_index] = value
        prev_values[sensor.output_index] = value
```

**Key Points:**
- **MUST** read value even if not keeping the sensor
- Skipping reads causes file position misalignment

## Change Tracking Process

### 1. Monitor C++ Repository

Track the upstream C++ repository for changes:

```bash
cd dbd2netcdf
git fetch origin
git log --oneline HEAD..origin/master
```

### 2. Identify Relevant Changes

Focus on changes to these files:
- `Decompress.C/H` - Decompression algorithm
- `Header.C/H` - Header parsing
- `Sensor.C/H`, `Sensors.C/H` - Sensor handling
- `KnownBytes.C/H` - Endianness detection
- `Data.C/H` - Binary data parsing
- `dbd2netCDF.C` - Command-line interface

### 3. Review and Apply Changes

For each relevant commit:

1. **Read the C++ diff:**
   ```bash
   git show <commit-hash>
   ```

2. **Identify the equivalent Python code:**
   - Use the file mapping above
   - Check CLAUDE.md for architecture details

3. **Implement the change:**
   - Update Python code to match new logic
   - Maintain Python idioms (don't translate literally)
   - Add comments referencing C++ commit if significant

4. **Test against C++ output:**
   ```bash
   python3 tests/test_dbd2nc.py
   ```

5. **Document in this file:**
   - Update "Last Sync" section below
   - Note any divergences from C++ implementation

### 4. Using the Sync Script

```bash
# Check for C++ changes
python3 scripts/check_cpp_changes.py

# This will:
# - Show commits since last sync
# - Identify changed files
# - Highlight files that map to Python code
```

## Last Sync

**Date:** January 2025
**C++ Commit:** Initial implementation
**Status:** Python implementation matches C++ functionality

**Known Differences:**
- Python uses xarray for NetCDF writing (vs C++ using netCDF library)
- Python uses numpy arrays (vs C++ using std::vector)
- Python has single-pass file reading (C++ can seek)
- Python integrates with xarray backend protocol

## Testing Against C++ Implementation

### Quick Verification

```bash
# Test single file
./scripts/compare_with_cpp.sh dbd_files/01330000.dcd

# Test all file types
./scripts/compare_with_cpp.sh dbd_files/*.{dcd,ecd,scd,tcd}
```

### Detailed Comparison

```bash
# Run full test suite
python3 tests/test_dbd2nc.py

# Check specific file type
python3 tests/test_dbd2nc.py --file-type dcd
```

### Manual Verification

```python
import xarray as xr
import numpy as np

# Load both outputs
ds_cpp = xr.open_dataset('cpp_output.nc', decode_timedelta=False)
ds_python = xr.open_dataset('python_output.nc', decode_timedelta=False)

# Compare
assert ds_cpp.sizes['i'] == ds_python.sizes['i']
for var in set(ds_cpp.data_vars) & set(ds_python.data_vars):
    np.testing.assert_allclose(
        np.squeeze(ds_cpp[var].values),
        np.squeeze(ds_python[var].values),
        rtol=1e-9, equal_nan=True
    )
```

## Reporting Issues

When C++ changes break Python compatibility:

1. Create an issue with:
   - C++ commit hash
   - Changed file(s)
   - Expected vs actual behavior
   - Failing test output

2. Reference this document's file mapping

3. Tag with `sync-needed` label

## Future Enhancements to Track

Monitor C++ repository for these potential additions:
- New sensor types or encoding formats
- Performance optimizations in data parsing
- Bug fixes in endianness handling or decompression
- Changes to cache file format
- New command-line options

## Version Compatibility

| Python Version | Compatible C++ Version | Notes |
|----------------|------------------------|-------|
| 0.1 | dbd2netCDF 1.6.x | Initial implementation |

