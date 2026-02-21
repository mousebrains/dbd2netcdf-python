# Synchronization Guide: C++ dbd2netCDF → xarray-dbd

This document describes the relationship between the upstream C++ dbd2netCDF source and the
copies in `csrc/`, and how to incorporate future changes.

## Overview

The C++ source files in `csrc/` are **copied** from the upstream
[dbd2netCDF](https://github.com/mousebrains/dbd2netcdf) repository (`src/` directory).
The only modification is replacing `spdlog` logging with a no-op `Logger.H` stub.
A pybind11 binding layer (`dbd_python.cpp`) exposes two functions to Python:
`read_dbd_file()` and `read_dbd_files()`.

## File Mapping

### Copied from upstream (csrc/ ← dbd2netCDF/src/)

| csrc/ file | Purpose |
|------------|---------|
| `ColumnData.H/.C` | Column-oriented typed data storage |
| `Data.H/.C` | Binary data record parsing |
| `Decompress.H/.C` | LZ4 frame decompression |
| `Header.H/.C` | ASCII header parsing |
| `KnownBytes.H/.C` | Endianness detection |
| `Sensor.H/.C` | Individual sensor metadata |
| `Sensors.H/.C` | Sensor collection management |
| `SensorsMap.H/.C` | Multi-file sensor merging |
| `lz4.c/.h` | Vendored LZ4 library |

### xarray-dbd specific (not in upstream)

| csrc/ file | Purpose |
|------------|---------|
| `dbd_python.cpp` | pybind11 bindings |
| `Logger.H` | No-op replacement for spdlog |

### Not copied (not needed)

| Upstream file | Reason |
|---------------|--------|
| `MyNetCDF.C/H` | Python uses xarray's NetCDF writer |
| `DataColumn.C/H` | Superseded by `ColumnData.C/H` |
| `Variables.C/H` | Handled by xarray Variable objects |
| `FileInfo.C/H` | Python pathlib sufficient |
| `StackDump.C/H` | Python traceback sufficient |
| `Tokenize.C/H` | Python str methods sufficient |
| `PD0.C/H`, `pd02netCDF.C` | PD0 ADCP format — out of scope |
| `SGMerge.C/H`, `sgMergeNetCDF.C` | Seaglider format — out of scope |

## Sync Process

### 1. Check for upstream changes

```bash
cd /Users/pat/tpw/dbd2netcdf
git fetch origin
git log --oneline HEAD..origin/master -- src/
```

### 2. Copy changed files

```bash
# Copy updated source files
cp /Users/pat/tpw/dbd2netcdf/src/ChangedFile.C csrc/
cp /Users/pat/tpw/dbd2netcdf/src/ChangedFile.H csrc/
```

### 3. Verify the build

```bash
pip install -e . && pytest
```

### 4. Update this document

Record the sync date and upstream commit below.

## Critical Implementation Details

These are key behaviors that must be preserved when syncing changes.

### Only load available sensors
```cpp
// Sensors.C — CRITICAL: only add sensors with available=true
if (sensor.qAvailable()) {
    mSensors.push_back(sensor);
}
```
Loading unavailable sensors causes index misalignment with binary data.

### Must read all new values
```cpp
// Data.C — MUST read value even if not keeping the sensor
const double value(sensor.read(is, kb));
```
Skipping reads desynchronizes the buffer offset.

### Tag search behavior
```cpp
// Data.C — don't stop on 'X' during forward search
// 'X' (0x58) appears in data values; only check at expected tag position
```

### LZ4 decompression
```cpp
// Decompress.C — frame size is big-endian uint16, buffer is 65536 bytes
const size_t n(((sz[0] << 8) & 0xff00) | (sz[1] & 0xff));
LZ4_decompress_safe(frame.data(), mBuffer, n, sizeof(mBuffer));
```

## Last Sync

**Date:** February 2025
**Method:** C++ source copied into `csrc/` with pybind11 bindings
**Status:** Full feature parity with dbd2netCDF for DBD reading

