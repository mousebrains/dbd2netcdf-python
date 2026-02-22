"""Integration tests for the xarray backend layer."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import xarray as xr
from conftest import CACHE_DIR, DBD_DIR, skip_no_data

import xarray_dbd as xdbd
from xarray_dbd.backend import DBDDataStore


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


class TestGuessCanOpen:
    """Tests for DBDBackendEntrypoint.guess_can_open()."""

    def test_valid_extensions(self):
        """All DBD extensions are recognized."""
        from xarray_dbd.backend import DBDBackendEntrypoint

        ep = DBDBackendEntrypoint()
        for ext in [".dbd", ".ebd", ".sbd", ".tbd", ".mbd", ".nbd",
                     ".dcd", ".ecd", ".scd", ".tcd", ".mcd", ".ncd"]:
            assert ep.guess_can_open(f"/tmp/file{ext}") is True

    def test_invalid_extensions(self):
        """Non-DBD extensions return False."""
        from xarray_dbd.backend import DBDBackendEntrypoint

        ep = DBDBackendEntrypoint()
        for ext in [".nc", ".csv", ".txt", ".nc4", ".hdf5"]:
            assert ep.guess_can_open(f"/tmp/file{ext}") is False

    def test_invalid_types(self):
        """Non-string/Path types return False."""
        from xarray_dbd.backend import DBDBackendEntrypoint

        ep = DBDBackendEntrypoint()
        assert ep.guess_can_open(None) is False
        assert ep.guess_can_open(123) is False
        assert ep.guess_can_open({"key": "val"}) is False


class TestOpenMultiConflictingMissions:
    """Tests for open_multi_dbd_dataset mission filter validation."""

    def test_conflicting_mission_filters(self):
        """skip_missions + keep_missions raises ValueError."""
        with pytest.raises(ValueError, match="Cannot specify both"):
            xdbd.open_multi_dbd_dataset(
                ["fake.dbd"],
                skip_missions=["a"],
                keep_missions=["b"],
            )


@skip_no_data
class TestWriteMultiDbdNetcdf:
    """Tests for write_multi_dbd_netcdf()."""

    def test_streaming_write(self):
        """write_multi_dbd_netcdf produces a valid NetCDF file."""
        files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        if len(files) < 2:
            pytest.skip("Need at least 2 .dcd files")

        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmpname = tmp.name

        try:
            n_records, n_files = xdbd.write_multi_dbd_netcdf(
                files,
                tmpname,
                skip_first_record=True,
                cache_dir=CACHE_DIR,
            )
            assert n_records > 0
            assert n_files >= 2

            ds = xr.open_dataset(tmpname, decode_timedelta=False)
            assert "i" in ds.dims
            assert len(ds.i) == n_records
            assert len(ds.data_vars) > 0
            ds.close()
        finally:
            Path(tmpname).unlink(missing_ok=True)

    def test_conflicting_mission_filters(self):
        """skip_missions + keep_missions raises ValueError."""
        with pytest.raises(ValueError, match="Cannot specify both"):
            xdbd.write_multi_dbd_netcdf(
                [DBD_DIR / "01330000.dcd"],
                "/tmp/never.nc",
                skip_missions=["a"],
                keep_missions=["b"],
                cache_dir=CACHE_DIR,
            )

    def test_empty_file_list(self):
        """Empty file list returns (0, 0)."""
        n_records, n_files = xdbd.write_multi_dbd_netcdf([], "/tmp/never.nc", cache_dir=CACHE_DIR)
        assert (n_records, n_files) == (0, 0)

    def test_no_matching_sensors(self):
        """to_keep with nonexistent sensors returns (0, 0)."""
        files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmpname = tmp.name
        try:
            n_records, n_files = xdbd.write_multi_dbd_netcdf(
                files,
                tmpname,
                to_keep=["totally_nonexistent_sensor_xyz"],
                cache_dir=CACHE_DIR,
            )
            assert (n_records, n_files) == (0, 0)
        finally:
            Path(tmpname).unlink(missing_ok=True)

    def test_to_keep_filter(self):
        """to_keep limits output variables."""
        files = sorted(DBD_DIR.glob("*.dcd"))[:2]
        if len(files) < 1:
            pytest.skip("No .dcd files available")

        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmpname = tmp.name
        try:
            n_records, n_files = xdbd.write_multi_dbd_netcdf(
                files,
                tmpname,
                to_keep=["m_present_time"],
                cache_dir=CACHE_DIR,
            )
            assert n_records > 0
            ds = xr.open_dataset(tmpname, decode_timedelta=False)
            assert list(ds.data_vars) == ["m_present_time"]
            ds.close()
        finally:
            Path(tmpname).unlink(missing_ok=True)

    def test_no_compression(self):
        """compression=0 produces valid output."""
        files = sorted(DBD_DIR.glob("*.dcd"))[:1]
        if not files:
            pytest.skip("No .dcd files available")

        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmpname = tmp.name
        try:
            n_records, n_files = xdbd.write_multi_dbd_netcdf(
                files,
                tmpname,
                compression=0,
                cache_dir=CACHE_DIR,
            )
            assert n_records > 0
            ds = xr.open_dataset(tmpname, decode_timedelta=False)
            assert len(ds.data_vars) > 0
            ds.close()
        finally:
            Path(tmpname).unlink(missing_ok=True)
