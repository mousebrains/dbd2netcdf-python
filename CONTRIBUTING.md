# Contributing to xarray-dbd

Thank you for your interest in contributing! This guide covers the development
workflow for xarray-dbd.

## Development Setup

**Prerequisites:** Python 3.13+, a C++17 compiler, and CMake ≥ 3.15.

```bash
git clone https://github.com/mousebrains/dbd2netcdf-python
cd dbd2netcdf-python
pip install -e ".[dev]"
```

The editable install compiles the C++ extension (`_dbd_cpp`) in-place.
Re-run `pip install -e .` after changing any C++ source in `csrc/`.

## Running Tests

```bash
pytest                       # all tests
pytest tests/test_backend.py # backend integration tests only
pytest -v --tb=short         # verbose with short tracebacks
```

Some tests require sample `.dbd`/`.dcd` files in `dbd_files/`. Tests that need
data are skipped automatically when the directory is absent.

## Linting and Formatting

```bash
ruff check xarray_dbd/ tests/ *.py   # lint
ruff format xarray_dbd/ tests/ *.py  # auto-format
mypy xarray_dbd/                     # type checking
```

Configuration for all tools lives in `pyproject.toml`. The project uses a
100-character line length.

## Syncing C++ Changes from Upstream

The C++ parser in `csrc/` is a copy of
[dbd2netCDF](https://github.com/mousebrains/dbd2netcdf) (`src/` directory).
To incorporate upstream changes:

1. Copy updated `.C` / `.H` files from `dbd2netcdf/src/` into `csrc/`.
2. Keep `csrc/Logger.H` (our no-op stub replacing spdlog) and
   `csrc/dbd_python.cpp` (pybind11 bindings) — these are local files.
3. Rebuild: `pip install -e .`
4. Run the full test suite and compare output against the C++ reference.

See `docs/SYNC.md` for the detailed file mapping.

## Project Layout

```
csrc/           C++ source (pybind11 extension)
xarray_dbd/    Python package (backend, CLI, public API)
tests/         pytest test suite
scripts/       Utility and debug scripts
docs/          Project documentation
conda/         Conda build recipe
```

## Pull Request Guidelines

- Keep changes focused — one logical change per PR.
- Add or update tests for any behavioral changes.
- Run `ruff check`, `ruff format`, and `mypy` before submitting.
- Include a clear description of *what* and *why* in the PR body.

## Commit Messages

Use conventional-style messages:

```
Fix sensor cache lookup for compressed .ccc files

The cache reader was only checking .cac files. Now tries .ccc
(LZ4-compressed) as a fallback, matching dbd2netCDF behavior.
```
