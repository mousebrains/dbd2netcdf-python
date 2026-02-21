# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**xarray-dbd** wraps the C++ [dbd2netCDF](https://github.com/mousebrains/dbd2netcdf) parser via pybind11 to read Slocum ocean glider Dinkum Binary Data (DBD) files with native xarray integration. The C++ source is copied into `csrc/` and compiled as a Python extension module (`_dbd_cpp`). The C++ reference implementation lives at `/Users/pat/tpw/dbd2netcdf/` and its `mkTwo.py` batch script is the reference for `mkone` behavior.

## Commands

```bash
# Install for development (compiles C++ extension)
pip install -e .

# Run tests
pytest
pytest tests/test_cpp_backend.py         # C++ backend tests

# Lint and format (ruff is configured in pyproject.toml, line-length=100)
ruff check xarray_dbd/ *.py
ruff format xarray_dbd/ *.py

# Convert single/multiple DBD files to NetCDF
dbd2nc -C cache -o output.nc input.dbd
dbd2nc -C cache -o output.nc --skipFirst *.dcd

# Batch process directories (walks recursively for *.?[bc]d files)
mkone --outputPrefix /path/to/output/ --cache /path/to/cache /path/to/raw/

# Test with sample data (if dbd_files/ directory exists)
dbd2nc -C dbd_files/cache -o /tmp/test.nc dbd_files/*.dcd
```

## Architecture

### Data Flow

```
xr.open_dataset("file.dbd", engine="dbd")
    → DBDBackendEntrypoint.open_dataset()
        → DBDDataStore(filename)
            → _dbd_cpp.read_dbd_file()  ← pybind11 C++ extension
        → get_variables() → dict[str, xr.Variable]
    → xr.Dataset

xdbd.open_multi_dbd_dataset(files)
    → _dbd_cpp.read_dbd_files()  ← two-pass C++ approach via SensorsMap
    → Python-side xr.Dataset construction
```

### C++ Extension (`csrc/`)

The `_dbd_cpp` module is built from C++ sources in `csrc/`, copied from `/Users/pat/tpw/dbd2netcdf/src/` with spdlog replaced by no-op `Logger.H` stub.

**Key files:**
- `dbd_python.cpp` — pybind11 bindings exposing `read_dbd_file()` and `read_dbd_files()`
- `ColumnData.H/.C` — Column-oriented typed data parser (native int8/int16/float32/float64)
- `Header.C`, `Sensor.C`, `Sensors.C`, `SensorsMap.C` — DBD header/sensor parsing
- `KnownBytes.C` — Endianness detection and byte-swapped reads
- `Decompress.C` + `lz4.c` — LZ4 decompression for compressed DBD files

**Build system:** scikit-build-core + CMake (see `CMakeLists.txt`, `pyproject.toml`)

### Python Layer (`xarray_dbd/`)

- `__init__.py` — Exports `read_dbd_file`, `read_dbd_files`, `open_dbd_dataset`, `open_multi_dbd_dataset`
- `backend.py` — `DBDDataStore`, `DBDBackendEntrypoint` for xarray engine integration

### Sensor Cache System

Factored DBD files reference sensors by CRC rather than embedding the list. Cache files live alongside the data:
- `.cac` — plain text sensor definitions
- `.ccc` — LZ4-compressed sensor definitions (same frame format as compressed DBD files)

Cache lookup: try `{crc}.cac` first, then `{crc}.ccc`. The reader generates `.cac` files when reading unfactored files.

### mkone Batch Processing

Discovers files via `os.walk()` with regex `r"[.]" + key + r"[bc]d$"` for keys `d/e/m/n/s/t`. Type `d` (flight data) gets special handling: sensors are partitioned into dbd/sci/other subsets and written as three separate NetCDF files. All types are processed sequentially (not threaded) to avoid memory exhaustion.

### DBD File Format

```
ASCII Header     →  key: value pairs (mission_name, sensor_list_crc, etc.)
Sensor List      →  "s: T/F index storage_index size name units" lines
                     OR factored reference (use cache file by CRC)
Known Bytes (16) →  's' 'a' 0x1234(int16) 123.456(f32) 123456789.12345(f64)
Data Records     →  'd' tag + header bits + values
                     2-bit codes per sensor: 0=absent, 1=repeat, 2=new value
End              →  'X' tag
```

## Critical Rules

1. **Only load `available=True` sensors** (flag `T` in cache/sensor lines). Loading `F` sensors causes index misalignment with binary data — the binary stream only contains values for available sensors.

2. **Never treat `X` as end-of-data during forward search**. When searching for the next `d` tag between records, `X` (0x58) appears in data values. Only check for `X` at the expected tag position.

3. **LZ4 decompression**: C++ `Decompress.C` uses `LZ4_decompress_safe()` with 65536-byte output buffer.

4. **Must read ALL sensor values (code==2) even if not keeping them**. Skipping unneeded values desynchronizes the buffer offset.

5. **`skip_first_record` semantics**: Files are sorted, first contributing file keeps all records, subsequent files drop their first record.

6. **Fill values**: NaN for float32/float64, -127 for int8, -32768 for int16 — matching the C++ dbd2netCDF standalone.

## Testing Against C++ Reference

- Compare record counts on dimension `i` — should match exactly for non-corrupted datasets
- Corrupted files: our per-file approach may recover fewer partial records than C++ standalone's single-pass approach
- C++ output includes `hdr_*` variables (10 extra vars) that Python doesn't produce — exclude from variable count comparison
- Use `np.allclose(equal_nan=True)` for float comparison; for int types, mask out NaN in C++ before comparing
- C++ reference output for the mariner dataset lives at `/Users/pat/tpw/mariner/tpw/`
- Run C++ via: `python3 /Users/pat/tpw/dbd2netcdf/mkTwo.py --outputPrefix /path/to/output/ /path/to/raw/`
