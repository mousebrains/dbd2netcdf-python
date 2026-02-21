"""Integration tests for the xarray backend layer."""

from pathlib import Path

import pytest
import xarray as xr

import xarray_dbd as xdbd
from xarray_dbd.backend import DBDDataStore

# Test data directory
DBD_DIR = Path(__file__).parent.parent / "dbd_files"
CACHE_DIR = str(DBD_DIR / "cache")

has_test_data = (DBD_DIR / "01330000.dcd").exists()
skip_no_data = pytest.mark.skipif(not has_test_data, reason="Test data not available")


@skip_no_data
class TestDBDDataStore:
    """Tests for DBDDataStore."""

    def test_basic_construction(self):
        store = DBDDataStore(DBD_DIR / "01330000.dcd", cache_dir=CACHE_DIR)
        assert store._n_records > 0
        assert len(store._sensor_names) > 0

    def test_get_variables(self):
        store = DBDDataStore(DBD_DIR / "01330000.dcd", cache_dir=CACHE_DIR)
        variables = store.get_variables()
        assert isinstance(variables, dict)
        assert len(variables) > 0
        for _name, var in variables.items():
            assert isinstance(var, xr.Variable)
            assert var.dims == ("i",)
            assert len(var) == store._n_records

    def test_get_attrs(self):
        store = DBDDataStore(DBD_DIR / "01330000.dcd", cache_dir=CACHE_DIR)
        attrs = store.get_attrs()
        assert "mission_name" in attrs
        assert "source_file" in attrs
        assert "01330000.dcd" in attrs["source_file"]

    def test_get_dimensions(self):
        store = DBDDataStore(DBD_DIR / "01330000.dcd", cache_dir=CACHE_DIR)
        dims = store.get_dimensions()
        assert "i" in dims
        assert dims["i"] == store._n_records

    def test_sensor_units_preserved(self):
        store = DBDDataStore(DBD_DIR / "01330000.dcd", cache_dir=CACHE_DIR)
        variables = store.get_variables()
        for var in variables.values():
            assert "units" in var.attrs

    def test_nonexistent_file(self):
        with pytest.raises(OSError, match="Failed to read"):
            DBDDataStore("/nonexistent/file.dbd", cache_dir=CACHE_DIR)

    def test_to_keep_filters(self):
        store_all = DBDDataStore(DBD_DIR / "01330000.dcd", cache_dir=CACHE_DIR)
        store_few = DBDDataStore(
            DBD_DIR / "01330000.dcd",
            cache_dir=CACHE_DIR,
            to_keep=["m_present_time"],
        )
        assert len(store_few._sensor_names) < len(store_all._sensor_names)


@skip_no_data
class TestOpenDbdDataset:
    """Tests for open_dbd_dataset()."""

    def test_returns_dataset(self):
        ds = xdbd.open_dbd_dataset(DBD_DIR / "01330000.dcd", cache_dir=CACHE_DIR)
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0

    def test_drop_variables(self):
        ds_all = xdbd.open_dbd_dataset(DBD_DIR / "01330000.dcd", cache_dir=CACHE_DIR)
        first_var = list(ds_all.data_vars)[0]
        ds_drop = xdbd.open_dbd_dataset(
            DBD_DIR / "01330000.dcd",
            cache_dir=CACHE_DIR,
            drop_variables=[first_var],
        )
        assert first_var not in ds_drop.data_vars
        assert len(ds_drop.data_vars) == len(ds_all.data_vars) - 1

    def test_xr_open_dataset_engine(self):
        ds = xr.open_dataset(DBD_DIR / "01330000.dcd", engine="dbd", cache_dir=CACHE_DIR)
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0


@skip_no_data
class TestOpenMultiDbdDataset:
    """Tests for open_multi_dbd_dataset()."""

    def test_multiple_files(self):
        files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        if len(files) < 2:
            pytest.skip("Need at least 2 .dcd files")
        ds = xdbd.open_multi_dbd_dataset(files, cache_dir=CACHE_DIR)
        assert isinstance(ds, xr.Dataset)
        assert ds.attrs["n_files"] >= 2

    def test_empty_file_list(self):
        ds = xdbd.open_multi_dbd_dataset([])
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) == 0

    def test_sensor_warning(self, caplog):
        """Requesting non-existent sensors logs a warning."""
        import logging

        files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        with caplog.at_level(logging.WARNING, logger="xarray_dbd.backend"):
            xdbd.open_multi_dbd_dataset(
                files,
                cache_dir=CACHE_DIR,
                to_keep=["totally_fake_sensor_xyz"],
            )
        assert "totally_fake_sensor_xyz" in caplog.text
