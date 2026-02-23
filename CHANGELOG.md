# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.3] - 2026-02-23

### Added

- `include_source` support in `MultiDBD.get()` — returns per-record source DBD references, matching dbdreader's API
- `continue_on_reading_error` parameter for `MultiDBD.get()` — skip corrupted files instead of raising, matching dbdreader v0.5.9
- `DBD_ERROR_READ_ERROR` error code (14) for compatibility with dbdreader
- Python 3.14 pre-built wheels for all platforms (Linux, macOS, Windows)
- Attribution to Lucas Merckelbach's [dbdreader](https://github.com/smerckel/dbdreader) in README

## [0.2.2] - 2026-02-23

### Added

- `preload` parameter for `DBD` and `MultiDBD` constructors
- Changelog configuration and tag/version validation in publish workflow

### Fixed

- mypy errors: `datetime.UTC`, tuple assignments, type annotations
- ruff formatting compliance

## [0.2.1] - 2026-02-22

### Added

- Streaming NetCDF writer (`write_multi_dbd_netcdf`) for low-memory batch conversion
- dbdreader-compatible API layer (`DBD` and `MultiDBD` classes in `xarray_dbd.dbdreader2`)
- Unified CLI under `xdbd` command with subcommands (`2nc`, `mkone`, `2csv`, `missions`, `cache`)
- Monotonicity check in `get_sync()` to prevent silent wrong results from `np.interp`

### Changed

- CLI restructured: standalone `dbd2nc` and `mkone` commands replaced by `xdbd 2nc` and `xdbd mkone`
- Streaming mode is now the default for non-append `2nc` and `mkone` (requires netCDF4)
- Fill values corrected: -127 for int8, -32768 for int16 (matching C++ dbd2netCDF standalone)
- Multi-file reader uses read-copy-discard strategy to reduce peak memory ~53%
- Replaced inf with NaN in float reads to match C++ dbd2netCDF behavior

### Fixed

- Multi-file parse dropping records from unfactored DBD files
- Corrupted file recovery: discard partial record on I/O error

## [0.1.0] - 2026-02-20

### Added

- C++ backend via pybind11 wrapping [dbd2netCDF](https://github.com/mousebrains/dbd2netcdf) parser
- Native xarray engine integration (`xr.open_dataset(f, engine="dbd")`)
- Multi-file reading with `open_multi_dbd_dataset()` using C++ SensorsMap two-pass approach
- CLI tools: `dbd2nc` for single/multi-file conversion, `mkone` for batch directory processing
- Native dtype support: int8, int16, float32, float64 columns (no double-conversion overhead)
- LZ4 decompression for compressed `.?cd` files
- Sensor filtering (`to_keep`), mission filtering (`skip_missions`/`keep_missions`)
- Corrupted file recovery with `repair=True`
- Python 3.10+ and free-threaded Python (PEP 703) support

### Changed

- Replaced pure-Python parser with C++ pybind11 extension for ~5x performance improvement
- Fill values: NaN for float32/float64, -127 for int8, -32768 for int16 (matching C++ dbd2netCDF)
