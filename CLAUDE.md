# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is **xarray-dbd**, a pure Python implementation of a DBD (Dinkum Binary Data) file reader for ocean glider data. It provides native xarray integration for reading Slocum glider files, replacing the need for the C++ dbd2netCDF tool.

The project consists of:
- **xarray_dbd/**: Core library providing xarray backend for DBD files
- **dbd2nc.py**: Command-line tool equivalent to C++ dbd2netCDF
- **mkOne.py**: Batch processing script for converting multiple file types

## Commands

### Development Setup
```bash
pip install -e ".[dev]"
```

### Testing
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_reader.py

# Run with coverage
pytest --cov=xarray_dbd
```

### Code Formatting
```bash
# Format code
black xarray_dbd/ *.py

# Check style
flake8 xarray_dbd/

# Format with specific line length
black --line-length 100 xarray_dbd/
```

### Running the Tools
```bash
# Convert DBD files to NetCDF
python3 dbd2nc.py -C cache -o output.nc input.dbd

# Batch process with mkOne.py
python3 mkOne.py --outputPrefix /path/to/output/ --cache cache *.dbd

# Test with sample files (if dbd_files/ exists)
python3 dbd2nc.py -C dbd_files/cache -o /tmp/test.nc dbd_files/*.dcd
```

## Architecture

### Core Data Flow

1. **File Reading** (`decompression.py` → `header.py` → `reader.py`)
   - Detect compressed files (.?c? extensions = LZ4 compressed)
   - Decompress LZ4 frames on-the-fly if needed
   - Parse ASCII header to extract metadata
   - Load sensor definitions from cache files (.cac) or read from file
   - Parse binary data with run-length encoding

2. **Sensor Management** (`sensor.py`)
   - **Critical**: Only sensors marked as "available" (flag='T') in cache files should be loaded
   - Sensors have: name, size (1/2/4/8 bytes), units, file_index
   - Filtering applied via `to_keep` (output sensors) and `criteria` (selection sensors)

3. **Backend Integration** (`backend.py`)
   - Implements xarray's `BackendEntrypoint` protocol
   - Provides `open_dbd_dataset()` and `open_multi_dbd_dataset()`
   - Creates xarray Dataset with dimension 'i' (and legacy 'j' with size 1)

### DBD File Format Structure

```
┌─────────────────────┐
│   ASCII Header      │  Mission metadata, sensor count, CRC
├─────────────────────┤
│   Sensor List       │  May be factored (binary) → use cache/.cac file
│   OR Binary Ref     │  OR text list (s: lines)
├─────────────────────┤
│   Known Bytes (16)  │  's' 'a' 0x1234 123.456f 123456789.12345d
├─────────────────────┤
│   Data Records      │  'd' tag + header bits + values
│   ...               │  2-bit codes: 0=none, 1=repeat, 2=new, 3=unused
│   'X' end tag       │
└─────────────────────┘
```

### Key Implementation Details

**LZ4 Decompression** (`decompression.py`):
- Compressed files have 2-byte big-endian frame size + LZ4 data
- `LZ4DecompressingReader` provides file-like interface
- Must specify `uncompressed_size=65536` for lz4.block.decompress()

**Sensor Cache Files** (`reader.py:_read_sensors()`):
- Factored sensor lists use `.cac` files named `{sensor_list_crc}.cac`
- **CRITICAL BUG FIX**: Only load sensors with `available=True` (flag 'T')
  - Loading all sensors causes index misalignment with binary data
  - This was a major bug that caused wrong values and truncated records

**Data Parsing** (`reader.py:_read_all_data()`):
- Records start with 'd' tag (0x64)
- Header bits: (n_sensors + 3) // 4 bytes encoding 2-bit codes
- Null bytes between records are normal (padding) - search forward for next 'd'
- **Never** stop on 'X' (0x58) found during search - it appears in data values
- Only check for 'X' end tag at expected tag position

**Endianness** (`reader.py:KnownBytes`):
- Detect from known bytes section using 0x1234 int16
- `flip_bytes=False` means little-endian, `True` means big-endian
- Validate with 123.456 float32 and 123456789.12345 float64

## Common Pitfalls

1. **Sensor Loading**: Always filter to `available=True` sensors from cache files
2. **Tag Search**: Don't treat 'X' as end tag during forward search between records
3. **LZ4 Decompression**: Must provide `uncompressed_size` parameter
4. **Record Skipping**: Must read ALL sensor values (code==2) even if not keeping them
5. **Dimension Compatibility**: Include singleton 'j' dimension for dbd2netCDF compatibility

## File Organization

```
xarray_dbd/
├── __init__.py       - Package exports, version
├── backend.py        - xarray BackendEntrypoint, open_dbd_dataset()
├── reader.py         - DBDReader class, read_multiple_dbd_files()
├── header.py         - DBDHeader for ASCII header parsing
├── sensor.py         - DBDSensor, DBDSensors classes
└── decompression.py  - LZ4DecompressingReader

dbd2nc.py             - CLI tool (argparse-based)
mkOne.py              - Batch processor with threading
```

## Testing Strategy

When testing against C++ dbd2netCDF:
- Compare record counts (dimension 'i')
- Compare all variable values with `np.allclose()` for floats
- Account for singleton 'j' dimension differences
- Use `squeeze()` before comparing array values
- Check for `hdr_*` variables only in C++ output (expected)

## Dependencies

- **numpy>=1.20**: Array operations
- **xarray>=2022.3.0**: Dataset interface
- **lz4>=3.0.0**: Decompression for .?c? files (critical for compressed formats)
