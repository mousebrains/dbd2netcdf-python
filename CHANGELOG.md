# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.6] - 2026-03-30

### Added

- `--list-sensors` flag for `dbd2nc` CLI to print available sensors without conversion
- `batch_size` parameter for `write_multi_dbd_netcdf()` (was hardcoded at 100)
- Signal handling in `mkone` — Ctrl+C now terminates child processes cleanly
- "Working with Glider Data" section in README (sensor discovery, time conversion, fill values)
- Tests for `get_CTD_sync`, `determine_ctd_type`, `get_global_time_range`, file ordering, batch boundaries

### Changed

- `get_sync()` logs interpolation failures at WARNING level instead of INFO
- Streaming writer logs summary when batches are skipped due to errors
- `set_time_limits()` accepts numeric epoch seconds in addition to date strings
- C++ `SensorsMap::setUpForData()` validates sensor byte sizes across files

### Fixed

- **Data loss in streaming writer**: removed Python-side double-skip at batch boundaries (C++ already handles `skip_first_record`)
- **dbdreader2 file ordering**: pass `presorted=True` to `read_dbd_files` so C++ respects chronological order from `DBDList.sort()`
- **mkone worker error propagation**: workers now exit non-zero on failure so parent detects errors
- **`_get_with_source` time ordering**: results now sorted by time for consistency with normal `get()` path
- **`sci_extensions` missing `.sbd`**: file pairing now recognizes `.sbd` as a science file type
- **`set_time_limits` falsy check**: epoch time 0 no longer causes spurious ValueError
- **inf-to-NaN for repeated values**: code=1 (repeat) now converts infinity consistently with code=2 (new value)
- Removed unused `"j"` dimension from `DBDDataStore.get_dimensions()`
- Fixed `--skip-first` help text (was stale after skip semantics change)
- Fixed README: CLI command names, removed false wildcard `to_keep` claim

## [0.2.5] - 2026-03-30

### Added

- `sort` parameter for `open_multi_dbd_dataset()` and `write_multi_dbd_netcdf()` with three modes: `"header_time"` (default, sort by `fileopen_time` from each file's DBD header), `"lexicographic"`, and `"none"` (preserve caller's order)
- `--sort` CLI flag for `dbd2nc`, `mkone`, and `2csv` commands
- `presorted` parameter for `read_dbd_files()` C++ binding to skip internal lexicographic sort when files are pre-sorted by Python
- `sensor_size` attribute on variables from `open_multi_dbd_dataset()`, matching single-file behavior
- `--skip-first` flag for `mkone` as consistent alias for the inverse `--keep-first`
- Duplicate file detection and deduplication with warning in multi-file functions
- Output directory auto-creation in `write_multi_dbd_netcdf()`
- "Choosing an API" and "Slocum File Types" sections in README
- Fill value and CF-compliance guidance in README Known Limitations

### Changed

- `skip_first_record` in `read_dbd_files()` now skips the first record of **all** files (including the first), matching Lucas Merckelbach's dbdreader behavior
- Streaming NetCDF writer keeps a single file handle open instead of reopening per batch

### Fixed

- File ordering for TWR-style filenames (e.g. `ce_1137-2026-085-1-10.dbd` incorrectly sorting before `-2.dbd` under lexicographic sort)
- `_parse_fileopen_time()` now logs a warning instead of silently sorting unparseable files to end
- `DBD.get_fileopen_time()` no longer raises on unparseable header values
- Thread-safe random number generator in C++ cache file creation
- Integer overflow guard in C++ column capacity doubling

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
