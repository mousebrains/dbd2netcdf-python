"""Shared fixtures and constants for xarray-dbd tests."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# Test data directory
DBD_DIR = Path(__file__).parent.parent / "dbd_files"
CACHE_DIR = str(DBD_DIR / "cache")

# C++ reference and raw data dirs — skip tests when env vars are not set
_cpp_ref = os.getenv("DBD_CPP_REF_DIR", "")
_raw = os.getenv("DBD_RAW_DIR", "")
CPP_REF_DIR = Path(_cpp_ref) if _cpp_ref else None
RAW_DIR = Path(_raw) if _raw else None

has_test_data = (DBD_DIR / "01330000.dcd").exists()
skip_no_data = pytest.mark.skipif(not has_test_data, reason="Test data not available")


@pytest.fixture()
def dbd_dir() -> Path:
    """Return the DBD test data directory, skipping if absent."""
    if not has_test_data:
        pytest.skip("Test data not available")
    return DBD_DIR


@pytest.fixture()
def cache_dir() -> str:
    """Return the cache directory path string, skipping if absent."""
    if not has_test_data:
        pytest.skip("Test data not available")
    return CACHE_DIR
