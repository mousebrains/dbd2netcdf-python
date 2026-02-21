# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- Python 3.13+ and free-threaded Python (PEP 703) support

### Changed

- Replaced pure-Python parser with C++ pybind11 extension for ~5x performance improvement
- Fill values: NaN for float32/float64, 0 for int8/int16 (matching C++ double-NaN semantics)
