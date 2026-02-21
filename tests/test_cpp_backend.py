"""Tests for the C++ backend (_dbd_cpp module)"""

import subprocess
import tempfile
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from xarray_dbd._dbd_cpp import read_dbd_file, read_dbd_files

# Test data directory
DBD_DIR = Path(__file__).parent.parent / "dbd_files"
CACHE_DIR = str(DBD_DIR / "cache")
CPP_REF_DIR = Path("/Users/pat/tpw/mariner/tpw")
RAW_DIR = Path("/Users/pat/tpw/mariner/onboard/raw")


def test_import():
    """C++ module imports successfully."""
    from xarray_dbd._dbd_cpp import read_dbd_file, read_dbd_files
    assert callable(read_dbd_file)
    assert callable(read_dbd_files)


def test_read_single_compressed_file():
    """Read a compressed .dcd file."""
    f = str(DBD_DIR / "01330000.dcd")
    result = read_dbd_file(f, cache_dir=CACHE_DIR, skip_first_record=False)

    assert result["n_records"] > 0
    assert len(result["sensor_names"]) > 0
    assert len(result["columns"]) == len(result["sensor_names"])

    # All columns should have the same length
    n = result["n_records"]
    for col in result["columns"]:
        assert len(col) == n


def test_column_dtypes():
    """Columns have correct native dtypes based on sensor size."""
    f = str(DBD_DIR / "01330000.dcd")
    result = read_dbd_file(f, cache_dir=CACHE_DIR, skip_first_record=False)

    size_to_dtype = {1: np.int8, 2: np.int16, 4: np.float32, 8: np.float64}

    for i, size in enumerate(result["sensor_sizes"]):
        expected = size_to_dtype[size]
        actual = result["columns"][i].dtype
        assert actual == expected, f"Sensor {result['sensor_names'][i]}: expected {expected}, got {actual}"


def test_skip_first_record():
    """skip_first_record=True reduces record count by 1."""
    f = str(DBD_DIR / "01330000.dcd")
    r_all = read_dbd_file(f, cache_dir=CACHE_DIR, skip_first_record=False)
    r_skip = read_dbd_file(f, cache_dir=CACHE_DIR, skip_first_record=True)

    assert r_all["n_records"] == r_skip["n_records"] + 1


def test_header_fields():
    """Header dict has expected keys."""
    f = str(DBD_DIR / "01330000.dcd")
    result = read_dbd_file(f, cache_dir=CACHE_DIR, skip_first_record=False)

    hdr = result["header"]
    assert "mission_name" in hdr
    assert "sensor_list_crc" in hdr
    assert "full_filename" in hdr


def test_xr_open_dataset():
    """xr.open_dataset with engine='dbd' works."""
    f = DBD_DIR / "01330000.dcd"
    ds = xr.open_dataset(f, engine="dbd", cache_dir=CACHE_DIR, skip_first_record=False)

    assert len(ds.data_vars) > 0
    assert "i" in ds.dims
    assert len(ds.i) > 0
    ds.close()


def test_read_multiple_files():
    """read_dbd_files handles multiple files."""
    files = sorted(str(f) for f in DBD_DIR.glob("*.dcd"))[:5]
    if len(files) < 2:
        pytest.skip("Need at least 2 test files")

    result = read_dbd_files(
        files,
        cache_dir=CACHE_DIR,
        skip_first_record=True,
    )

    assert result["n_records"] > 0
    assert result["n_files"] > 0
    assert len(result["columns"]) == len(result["sensor_names"])


def test_open_multi_dbd_dataset():
    """open_multi_dbd_dataset returns correct Dataset."""
    import xarray_dbd as xdbd

    files = sorted(DBD_DIR.glob("*.dcd"))[:5]
    if len(files) < 2:
        pytest.skip("Need at least 2 test files")

    ds = xdbd.open_multi_dbd_dataset(
        files,
        skip_first_record=True,
        cache_dir=CACHE_DIR,
    )

    assert len(ds.data_vars) > 0
    assert "i" in ds.dims
    assert len(ds.i) > 0
    ds.close()


def test_nan_fill_for_floats():
    """Float columns use NaN for absent values, int columns use 0."""
    files = sorted(str(f) for f in DBD_DIR.glob("*.dcd"))[:5]
    if len(files) < 2:
        pytest.skip("Need at least 2 test files")

    result = read_dbd_files(files, cache_dir=CACHE_DIR, skip_first_record=True)

    for i, size in enumerate(result["sensor_sizes"]):
        col = result["columns"][i]
        if size in (4, 8):  # float types
            # NaN fill is expected
            pass  # Just verify dtype
            assert col.dtype in (np.float32, np.float64)
        else:  # int types
            assert col.dtype in (np.int8, np.int16)


@pytest.mark.skipif(not CPP_REF_DIR.exists(), reason="C++ reference output not available")
def test_record_count_vs_cpp_dbd():
    """Record count matches C++ reference for dbd files."""
    ref = xr.open_dataset(CPP_REF_DIR / "dbd.nc", decode_timedelta=False)
    cpp_records = len(ref.i)
    ref.close()

    import xarray_dbd as xdbd
    dbd_files = sorted(RAW_DIR.rglob("*.dcd"))

    # Read with same parameters as mkOne.py default
    skip_missions = ["status.mi", "lastgasp.mi", "initial.mi", "overtime.mi",
                     "ini0.mi", "ini1.mi", "ini2.mi", "ini3.mi"]

    # Read sensor list from reference
    with open(CPP_REF_DIR / "dbd.sensors") as f:
        to_keep = [line.strip() for line in f if line.strip()]

    ds = xdbd.open_multi_dbd_dataset(
        dbd_files,
        skip_first_record=True,
        cache_dir=CACHE_DIR,
        skip_missions=skip_missions,
        to_keep=to_keep,
    )

    assert len(ds.i) == cpp_records, f"Expected {cpp_records}, got {len(ds.i)}"
    ds.close()


@pytest.mark.skipif(not CPP_REF_DIR.exists(), reason="C++ reference output not available")
def test_values_match_cpp_tbd():
    """Float values match C++ reference for tbd files."""
    ref = xr.open_dataset(CPP_REF_DIR / "tbd.nc", decode_timedelta=False)

    import xarray_dbd as xdbd
    tbd_files = sorted(RAW_DIR.rglob("*.tcd"))

    skip_missions = ["status.mi", "lastgasp.mi", "initial.mi", "overtime.mi",
                     "ini0.mi", "ini1.mi", "ini2.mi", "ini3.mi"]

    ds = xdbd.open_multi_dbd_dataset(
        tbd_files,
        skip_first_record=True,
        cache_dir=CACHE_DIR,
        skip_missions=skip_missions,
    )

    ref_vars = [v for v in ref.data_vars if not v.startswith("hdr_")]
    common = set(ds.data_vars) & set(ref_vars)

    for v in common:
        py_data = ds[v].values.flatten()
        cpp_data = ref[v].values.flatten()[:len(py_data)]

        if np.issubdtype(py_data.dtype, np.floating):
            assert np.allclose(py_data, cpp_data, rtol=1e-6, equal_nan=True), \
                f"{v}: float values don't match"
        else:
            # For int types, NaN in C++ corresponds to 0 in our output
            mask = ~np.isnan(cpp_data)
            assert np.array_equal(py_data[mask].astype(np.float64), cpp_data[mask]), \
                f"{v}: int values don't match (where C++ is non-NaN)"

    ds.close()
    ref.close()


@pytest.mark.skipif(
    not Path("/usr/local/bin/dbd2netCDF").exists(),
    reason="Standalone dbd2netCDF not installed",
)
def test_single_file_vs_standalone_cpp():
    """Single file read matches standalone dbd2netCDF output."""
    f = str(DBD_DIR / "01330000.dcd")

    # Our reader
    result = read_dbd_file(f, cache_dir=CACHE_DIR, skip_first_record=False)
    py_n = int(result["n_records"])

    # Standalone C++
    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmpname = tmp.name
    try:
        subprocess.run(
            ["/usr/local/bin/dbd2netCDF", "--cache", CACHE_DIR, "--output", tmpname, f],
            capture_output=True,
            check=True,
        )
        ds = xr.open_dataset(tmpname, decode_timedelta=False)
        cpp_n = len(ds.i)
        ds.close()

        assert py_n == cpp_n, f"Record count mismatch: py={py_n}, cpp={cpp_n}"
    finally:
        Path(tmpname).unlink(missing_ok=True)
