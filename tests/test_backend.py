"""Integration tests for the xarray backend layer."""

from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from conftest import CACHE_DIR, DBD_DIR, skip_no_data

import xarray_dbd as xdbd
from xarray_dbd.backend import DBDDataStore, _resolve_cache_dir


class TestResolveCacheDir:
    """Unit tests for _resolve_cache_dir (does not need sample data)."""

    def test_explicit_missing_raises(self, tmp_path):
        """An explicit cache_dir that doesn't exist → FileNotFoundError."""
        missing = tmp_path / "nope"
        with pytest.raises(FileNotFoundError, match="Cache directory not found"):
            _resolve_cache_dir(str(missing), tmp_path)

    def test_explicit_present_returns_path(self, tmp_path):
        cache = tmp_path / "cache"
        cache.mkdir()
        assert _resolve_cache_dir(cache, tmp_path) == str(cache)

    def test_none_with_missing_fallback_returns_empty(self, tmp_path):
        """No user value + no <parent>/cache → empty string (no-cache mode)."""
        assert _resolve_cache_dir(None, tmp_path) == ""

    def test_none_with_present_fallback(self, tmp_path):
        """<parent>/cache exists → returned."""
        (tmp_path / "cache").mkdir()
        assert _resolve_cache_dir(None, tmp_path) == str(tmp_path / "cache")


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
        for ext in [
            ".dbd",
            ".ebd",
            ".sbd",
            ".tbd",
            ".mbd",
            ".nbd",
            ".dcd",
            ".ecd",
            ".scd",
            ".tcd",
            ".mcd",
            ".ncd",
        ]:
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

    def test_all_batches_fail_raises_oserror(self, tmp_path, monkeypatch):
        """If every read_dbd_files call raises, the writer raises OSError."""
        files = sorted(DBD_DIR.glob("*.dcd"))[:2]
        if len(files) < 2:
            pytest.skip("Need at least 2 .dcd files")
        out = tmp_path / "all_fail.nc"

        from xarray_dbd import backend as _backend

        def _boom(*args, **kwargs):  # matches read_dbd_files signature
            raise RuntimeError("simulated batch failure")

        monkeypatch.setattr(_backend, "read_dbd_files", _boom)

        with pytest.raises(OSError, match="No DBD records written"):
            xdbd.write_multi_dbd_netcdf(
                files,
                out,
                cache_dir=CACHE_DIR,
                batch_size=1,  # force multiple batches
            )

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

    def test_batch_boundary_no_record_loss(self):
        """Small batch_size should produce same record count as large batch."""
        files = sorted(DBD_DIR.glob("*.dcd"))
        if len(files) < 3:
            pytest.skip("Need at least 3 files for batch boundary test")

        with (
            tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as t1,
            tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as t2,
        ):
            p1, p2 = t1.name, t2.name
        try:
            n1, _ = xdbd.write_multi_dbd_netcdf(
                files,
                p1,
                cache_dir=CACHE_DIR,
                batch_size=1,
                to_keep=["m_present_time"],
            )
            n2, _ = xdbd.write_multi_dbd_netcdf(
                files,
                p2,
                cache_dir=CACHE_DIR,
                batch_size=1000,
                to_keep=["m_present_time"],
            )
            assert n1 == n2, f"batch_size=1 gave {n1} records, batch_size=1000 gave {n2}"
        finally:
            Path(p1).unlink(missing_ok=True)
            Path(p2).unlink(missing_ok=True)

    def test_round_trip_values(self):
        """Write to NetCDF and read back — sensor values must match."""
        files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        if not files:
            pytest.skip("No .dcd files available")
        sensor = "m_present_time"

        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmpname = tmp.name
        try:
            n_records, _ = xdbd.write_multi_dbd_netcdf(
                files,
                tmpname,
                cache_dir=CACHE_DIR,
                to_keep=[sensor],
            )
            assert n_records > 0
            ds = xr.open_dataset(tmpname, decode_timedelta=False)
            written = ds[sensor].values
            ds.close()

            # Compare with in-memory dataset
            ds_mem = xdbd.open_multi_dbd_dataset(files, cache_dir=CACHE_DIR, to_keep=[sensor])
            expected = ds_mem[sensor].values

            assert len(written) == len(expected)
            np.testing.assert_allclose(written, expected, equal_nan=True)
        finally:
            Path(tmpname).unlink(missing_ok=True)

    def test_cross_api_consistency(self):
        """open_multi_dbd_dataset and write_multi_dbd_netcdf produce same data."""
        files = sorted(DBD_DIR.glob("*.dcd"))[:3]
        if not files:
            pytest.skip("No .dcd files available")
        sensors = ["m_present_time", "m_depth"]

        ds_mem = xdbd.open_multi_dbd_dataset(files, cache_dir=CACHE_DIR, to_keep=sensors)

        with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
            tmpname = tmp.name
        try:
            xdbd.write_multi_dbd_netcdf(
                files,
                tmpname,
                cache_dir=CACHE_DIR,
                to_keep=sensors,
            )
            ds_nc = xr.open_dataset(tmpname, decode_timedelta=False)
            for s in sensors:
                if s in ds_mem.data_vars and s in ds_nc.data_vars:
                    np.testing.assert_allclose(
                        ds_mem[s].values,
                        ds_nc[s].values,
                        equal_nan=True,
                        err_msg=f"Mismatch for {s}",
                    )
            ds_nc.close()
        finally:
            Path(tmpname).unlink(missing_ok=True)
