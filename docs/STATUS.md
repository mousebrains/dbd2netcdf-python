# xarray-dbd Implementation Status

## Architecture

xarray-dbd wraps the C++ [dbd2netCDF](https://github.com/mousebrains/dbd2netcdf) parser via
pybind11 to read Slocum ocean glider Dinkum Binary Data (DBD) files with native xarray
integration. The C++ source is copied into `csrc/` and compiled as a Python extension module
(`_dbd_cpp`).

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

### Build System

- **scikit-build-core** + **CMake** compile `csrc/*.C` and `csrc/*.cpp` into `_dbd_cpp.so`
- **pybind11** provides the Python ↔ C++ binding layer
- Vendored **lz4** (`csrc/lz4.c`) handles compressed DBD files

### Package Structure

```
xarray_dbd/
├── __init__.py          Public API exports
├── backend.py           DBDDataStore, DBDBackendEntrypoint, open_*() functions
├── cli/
│   ├── dbd2nc.py        Single/multi-file DBD → NetCDF converter
│   └── mkone.py         Batch directory processor (mkTwo.py equivalent)
└── py.typed             PEP 561 marker

csrc/
├── dbd_python.cpp       pybind11 bindings (read_dbd_file, read_dbd_files)
├── ColumnData.H/.C      Column-oriented typed data parser
├── Header.C             ASCII header parsing
├── Sensor.C/.H          Individual sensor metadata
├── Sensors.C/.H         Sensor collection
├── SensorsMap.C/.H      Multi-file sensor merging
├── KnownBytes.C/.H      Endianness detection
├── Decompress.C/.H      LZ4 frame decompression
├── Data.C/.H            Binary data record parsing
└── lz4.c/.h             Vendored LZ4 library
```

## What's Implemented

### Core Features (✅ Complete)
- Native xarray integration via `BackendEntrypoint`
- C++ binary parsing of all DBD format variants
- Endianness detection via "known bytes" validation
- Run-length encoding (2-bit codes: absent/repeat/new-value)
- LZ4 decompression for compressed `.?cd` files
- Sensor cache system (`.cac` and `.ccc` files)
- Multi-file concatenation via `SensorsMap`
- Sensor filtering (`to_keep`)
- Mission filtering (`skip_missions`, `keep_missions`)
- Criteria-based record selection
- Skip-first-record deduplication
- Corrupted file repair mode
- Native dtype support (int8, int16, float32, float64)
- CLI tools: `dbd2nc` and `mkone`

### File Types Supported
All Slocum glider binary formats: `.dbd`, `.ebd`, `.sbd`, `.tbd`, `.mbd`, `.nbd`
and their compressed variants: `.dcd`, `.ecd`, `.scd`, `.tcd`, `.mcd`, `.ncd`

### Testing
- C++ backend unit tests (`tests/test_cpp_backend.py`)
- CLI smoke tests (`tests/test_cli.py`)
- Validated against C++ dbd2netCDF reference output

## Synchronization with C++ Upstream

The C++ source in `csrc/` is copied from the upstream dbd2netCDF repository with
`spdlog` replaced by a no-op `Logger.H` stub. See [SYNC.md](SYNC.md) for the file
mapping and change-tracking process.

## Known Limitations

- **Python 3.13+ required** — uses modern Python features and typing
- **Free-threaded Python (3.13t)**: pybind11 does not yet fully support `Py_GIL_DISABLED`;
  the extension segfaults under free-threaded builds
- No lazy/chunked loading — entire file is read into memory
- No direct timestamp parsing — users must handle `m_present_time` conversion
