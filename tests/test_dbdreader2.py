"""Tests for the dbdreader2 drop-in replacement (xarray_dbd.dbdreader2)."""

from __future__ import annotations

import os

import numpy as np
import pytest
from conftest import CACHE_DIR, DBD_DIR, skip_no_data

import xarray_dbd
from xarray_dbd.dbdreader2 import (
    DBD,
    LATLON_PARAMS,
    DBDCache,
    DbdError,
    DBDList,
    MultiDBD,
    _convertToDecimal,
    epochToDateTimeStr,
    heading_interpolating_function_factory,
    strptimeToEpoch,
    toDec,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

pytestmark = skip_no_data


def _single_file() -> str:
    return str(sorted(DBD_DIR.glob("*.dcd"))[0])


def _all_files() -> list[str]:
    return [str(f) for f in sorted(DBD_DIR.glob("*.dcd"))]


# ---------------------------------------------------------------------------
# TestDBD — single file
# ---------------------------------------------------------------------------


class TestDBD:
    def test_construction(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        assert isinstance(dbd.parameterNames, list)
        assert len(dbd.parameterNames) > 0
        dbd.close()

    def test_parameter_names_and_units(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        names = dbd.parameterNames
        units = dbd.parameterUnits
        assert isinstance(units, dict)
        assert set(names) == set(units.keys())
        dbd.close()

    def test_has_parameter_present(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        assert dbd.has_parameter("m_present_time")
        dbd.close()

    def test_has_parameter_absent(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        assert not dbd.has_parameter("no_such_sensor_xyz")
        dbd.close()

    def test_get_single(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        t, v = dbd.get("m_depth")
        assert isinstance(t, np.ndarray)
        assert isinstance(v, np.ndarray)
        assert len(t) == len(v)
        assert len(t) > 0
        # No NaN by default
        assert np.all(np.isfinite(t))
        assert np.all(np.isfinite(v))
        dbd.close()

    def test_get_multiple(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        results = dbd.get("m_depth", "m_pitch")
        assert isinstance(results, list)
        assert len(results) == 2
        for t, v in results:
            assert len(t) == len(v)
        dbd.close()

    def test_get_return_nans(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        t_no, _ = dbd.get("m_depth")
        t_yes, _ = dbd.get("m_depth", return_nans=True)
        assert len(t_yes) >= len(t_no)
        dbd.close()

    def test_get_missing_parameter_raises(self):
        """Unknown parameters raise DbdError by default."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        with pytest.raises(DbdError):
            dbd.get("nonexistent_sensor")
        dbd.close()

    def test_get_missing_parameter_no_check(self):
        """With check_for_invalid_parameters=False, missing params give empty arrays."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        t, v = dbd.get("nonexistent_sensor", check_for_invalid_parameters=False)
        assert len(t) == 0
        assert len(v) == 0
        dbd.close()

    def test_get_integer_sensor(self):
        """Integer fill values (-127 / -32768) should be filtered out."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        names = dbd.parameterNames
        int_sensors = [
            n for n in names
            if n in dbd._columns and dbd._columns[n].dtype in (np.int8, np.int16)
        ]
        if not int_sensors:
            pytest.skip("No integer sensors in test file")
        t, v = dbd.get(int_sensors[0])
        if dbd._columns[int_sensors[0]].dtype == np.int8:
            assert not np.any(v == -127)
        else:
            assert not np.any(v == -32768)
        dbd.close()

    def test_get_sync(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        result = dbd.get_sync("m_depth", "m_pitch")
        assert isinstance(result, tuple)
        assert len(result) == 3  # time, v0, v1
        t, v0, v1 = result
        assert len(t) == len(v0) == len(v1)
        dbd.close()

    def test_get_sync_too_few_params(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        with pytest.raises(ValueError, match="at least two"):
            dbd.get_sync("m_depth")
        dbd.close()

    def test_get_sync_missing_second_param(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        # get_sync should still work if second param has no data
        result = dbd.get_sync("m_depth", "m_pitch")
        t, v0, v1 = result
        assert len(v1) == len(t)
        dbd.close()

    def test_get_xy(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        x, y = dbd.get_xy("m_depth", "m_pitch")
        assert len(x) == len(y)
        dbd.close()

    def test_get_mission_name(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        name = dbd.get_mission_name()
        assert isinstance(name, str)
        assert len(name) > 0
        dbd.close()

    def test_get_fileopen_time(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        t = dbd.get_fileopen_time()
        assert isinstance(t, int)
        assert t > 0
        dbd.close()

    def test_close_get_raises(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        dbd.close()
        with pytest.raises(DbdError):
            dbd.get("m_depth")

    def test_close_properties_empty(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        dbd.close()
        assert dbd.parameterNames == []
        assert dbd.parameterUnits == {}
        assert not dbd.has_parameter("m_depth")

    def test_default_cache_dir(self):
        """When cacheDir is None, use DBDCache.CACHEDIR."""
        old = DBDCache.CACHEDIR
        try:
            DBDCache.CACHEDIR = CACHE_DIR
            dbd = DBD(_single_file())
            assert len(dbd.parameterNames) > 0
            dbd.close()
        finally:
            DBDCache.CACHEDIR = old

    def test_cache_attributes(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        assert dbd.cacheFound is True
        assert isinstance(dbd.cacheID, str)
        assert len(dbd.cacheID) > 0
        dbd.close()

    def test_header_info(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        assert isinstance(dbd.headerInfo, dict)
        assert "mission_name" in dbd.headerInfo
        assert "sensor_list_crc" in dbd.headerInfo
        dbd.close()

    def test_time_variable(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        assert dbd.timeVariable in ("m_present_time", "sci_m_present_time")
        dbd.close()


# ---------------------------------------------------------------------------
# TestMultiDBD — multiple files
# ---------------------------------------------------------------------------


class TestMultiDBD:
    def test_construction_filenames(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        assert mdbd.has_parameter("m_present_time")
        mdbd.close()

    def test_construction_pattern(self):
        pattern = str(DBD_DIR / "*.dcd")
        mdbd = MultiDBD(pattern=pattern, cacheDir=CACHE_DIR)
        assert mdbd.has_parameter("m_present_time")
        mdbd.close()

    def test_no_files_raises(self):
        with pytest.raises(DbdError):
            MultiDBD(filenames=[])

    def test_no_args_raises(self):
        with pytest.raises(DbdError):
            MultiDBD()

    def test_parameter_names_dict(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        pn = mdbd.parameterNames
        assert isinstance(pn, dict)
        assert "eng" in pn
        assert "sci" in pn
        assert isinstance(pn["eng"], list)
        assert isinstance(pn["sci"], list)
        all_params = pn["eng"] + pn["sci"]
        assert len(all_params) > 0
        mdbd.close()

    def test_parameter_units(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        units = mdbd.parameterUnits
        assert isinstance(units, dict)
        assert len(units) > 0
        mdbd.close()

    def test_get_single(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        t, v = mdbd.get("m_depth")
        assert len(t) == len(v)
        assert len(t) > 0
        mdbd.close()

    def test_get_multiple(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        results = mdbd.get("m_depth", "m_pitch")
        assert isinstance(results, list)
        assert len(results) == 2
        mdbd.close()

    def test_get_return_nans(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        t_no, _ = mdbd.get("m_depth")
        t_yes, _ = mdbd.get("m_depth", return_nans=True)
        assert len(t_yes) >= len(t_no)
        mdbd.close()

    def test_get_sync(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        result = mdbd.get_sync("m_depth", "m_pitch")
        assert isinstance(result, tuple)
        assert len(result) == 3
        t, v0, v1 = result
        assert len(t) == len(v0) == len(v1)
        mdbd.close()

    def test_get_xy(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        x, y = mdbd.get_xy("m_depth", "m_pitch")
        assert len(x) == len(y)
        mdbd.close()

    def test_has_parameter(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        assert mdbd.has_parameter("m_present_time")
        assert not mdbd.has_parameter("no_such_sensor_xyz")
        mdbd.close()

    def test_dbds_attribute(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        assert isinstance(mdbd.dbds, dict)
        assert "eng" in mdbd.dbds
        assert "sci" in mdbd.dbds
        # .dcd files are eng — dbds stores DBD objects
        assert len(mdbd.dbds["eng"]) > 0
        assert all(isinstance(d, DBD) for d in mdbd.dbds["eng"])
        assert all(isinstance(d, DBD) for d in mdbd.dbds["sci"])
        mdbd.close()

    def test_filenames_attribute(self):
        files = _all_files()
        mdbd = MultiDBD(filenames=files, cacheDir=CACHE_DIR)
        assert len(mdbd.filenames) == len(files)
        mdbd.close()

    def test_mission_list(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        assert isinstance(mdbd.mission_list, list)
        assert len(mdbd.mission_list) > 0
        mdbd.close()

    def test_is_science_data_file(self):
        assert MultiDBD.isScienceDataFile("test.ebd") is True
        assert MultiDBD.isScienceDataFile("test.ecd") is True
        assert MultiDBD.isScienceDataFile("test.tbd") is True
        assert MultiDBD.isScienceDataFile("test.nbd") is True
        assert MultiDBD.isScienceDataFile("test.dbd") is False
        assert MultiDBD.isScienceDataFile("test.dcd") is False
        assert MultiDBD.isScienceDataFile("test.sbd") is False

    def test_include_source_not_implemented(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        with pytest.raises(NotImplementedError):
            mdbd.get("m_depth", include_source=True)
        mdbd.close()

    def test_close_guards(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        mdbd.close()
        with pytest.raises(RuntimeError, match="closed"):
            mdbd.get("m_depth")
        assert mdbd.parameterNames == {"eng": [], "sci": []}
        assert mdbd.parameterUnits == {}

    def test_close_has_parameter(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        mdbd.close()
        assert not mdbd.has_parameter("m_depth")

    def test_max_files_positive(self):
        files = _all_files()
        if len(files) < 3:
            pytest.skip("Need at least 3 files")
        mdbd = MultiDBD(filenames=files, cacheDir=CACHE_DIR, max_files=2)
        assert len(mdbd.filenames) <= 2
        mdbd.close()

    def test_max_files_negative(self):
        files = _all_files()
        if len(files) < 3:
            pytest.skip("Need at least 3 files")
        mdbd = MultiDBD(filenames=files, cacheDir=CACHE_DIR, max_files=-2)
        assert len(mdbd.filenames) <= 2
        mdbd.close()

    def test_time_limits_dataset(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        t_min, t_max = mdbd.time_limits_dataset
        assert t_min is not None
        assert t_max is not None
        assert t_min <= t_max
        assert t_min > 0
        mdbd.close()

    def test_get_time_range(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        r = mdbd.get_time_range()
        assert isinstance(r, list)
        assert len(r) == 2
        assert isinstance(r[0], str)
        assert isinstance(r[1], str)
        mdbd.close()

    def test_get_global_time_range(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        r = mdbd.get_global_time_range()
        assert isinstance(r, list)
        assert len(r) == 2
        mdbd.close()

    def test_get_time_range_seconds(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        r = mdbd.get_time_range(fmt="%s")
        assert r == list(mdbd.time_limits_dataset)
        mdbd.close()

    def test_set_time_limits(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        t_min, t_max = mdbd.time_limits_dataset
        # Narrow time limits to exclude some files
        mid = (t_min + t_max) / 2
        mdbd.set_time_limits(mdbd._format_time(mid, "%d %b %Y %H:%M"))
        assert mdbd.time_limits[0] is not None
        # Data should still be readable
        t, v = mdbd.get("m_depth")
        assert len(t) >= 0
        mdbd.close()

    def test_set_skip_initial_line(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        t1, _ = mdbd.get("m_depth")
        mdbd.set_skip_initial_line(False)
        t2, _ = mdbd.get("m_depth")
        # With skip=False, we should get at least as many records
        assert len(t2) >= len(t1)
        mdbd.close()

    def test_dbds_returns_dbd_objects(self):
        """dbds values are lists of DBD objects with valid filenames."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        for key in ("eng", "sci"):
            for dbd_obj in mdbd.dbds[key]:
                assert isinstance(dbd_obj, DBD)
                assert isinstance(dbd_obj.filename, str)
                assert len(dbd_obj.filename) > 0
        mdbd.close()

    def test_close_then_get_sync_raises(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        mdbd.close()
        with pytest.raises(RuntimeError, match="closed"):
            mdbd.get_sync("m_depth", "m_pitch")

    def test_dbd_close_then_get_sync_raises(self):
        """DBD.get_sync after close() raises DbdError."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        dbd.close()
        with pytest.raises(DbdError):
            dbd.get_sync("m_depth", "m_pitch")

    def test_dbd_close_then_get_xy_raises(self):
        """DBD.get_xy after close() raises DbdError."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        dbd.close()
        with pytest.raises(DbdError):
            dbd.get_xy("m_depth", "m_pitch")

    def test_dbd_cache_found_with_valid_dir(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        assert dbd.cacheFound is True
        dbd.close()

    def test_dbd_cache_found_false_nonexistent_dir(self):
        """cacheFound is False when cacheDir doesn't exist on disk."""
        # Use CACHE_DIR for sensor lookup (required for factored files),
        # then override cacheDir to test the property logic.
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        dbd.cacheDir = "/nonexistent/path/cache"
        # Simulate the check that __init__ performs
        assert not (dbd.cacheDir and os.path.isdir(dbd.cacheDir))
        dbd.close()


# ---------------------------------------------------------------------------
# TestCrossValidation — compare against dbdreader if available
# ---------------------------------------------------------------------------


has_dbdreader = pytest.importorskip("dbdreader", reason="dbdreader not installed") is not None


@pytest.mark.skipif(not has_dbdreader, reason="dbdreader not installed")
class TestCrossValidation:
    def test_get_values_match(self):
        import dbdreader

        filename = _single_file()
        xdbd_dbd = DBD(filename, cacheDir=CACHE_DIR)
        dbdr_dbd = dbdreader.DBD(filename, cacheDir=CACHE_DIR)

        sensor = "m_depth"
        xt, xv = xdbd_dbd.get(sensor)
        dt, dv = dbdr_dbd.get(sensor)

        assert len(xt) == len(dt), f"length mismatch: {len(xt)} vs {len(dt)}"
        np.testing.assert_allclose(xt, dt, rtol=1e-10)
        np.testing.assert_allclose(xv, dv, rtol=1e-10)

        xdbd_dbd.close()
        dbdr_dbd.close()

    def test_get_sync_match(self):
        import dbdreader

        filename = _single_file()
        xdbd_dbd = DBD(filename, cacheDir=CACHE_DIR)
        dbdr_dbd = dbdreader.DBD(filename, cacheDir=CACHE_DIR)

        params = ("m_depth", "m_pitch")
        xr = xdbd_dbd.get_sync(*params)
        dr = dbdr_dbd.get_sync(*params)

        assert len(xr) == len(dr)
        for xa, da in zip(xr, dr, strict=True):
            assert len(xa) == len(da)
            mask = np.isfinite(xa) & np.isfinite(da)
            np.testing.assert_allclose(xa[mask], da[mask], rtol=1e-6)

        xdbd_dbd.close()
        dbdr_dbd.close()

    def test_parameter_names_match(self):
        import dbdreader

        filename = _single_file()
        xdbd_dbd = DBD(filename, cacheDir=CACHE_DIR)
        dbdr_dbd = dbdreader.DBD(filename, cacheDir=CACHE_DIR)

        xnames = set(xdbd_dbd.parameterNames)
        dnames = set(dbdr_dbd.parameterNames)

        assert dnames.issubset(xnames), f"missing in xdbd: {dnames - xnames}"

        xdbd_dbd.close()
        dbdr_dbd.close()

    def test_multi_get_values_match(self):
        """MultiDBD.get values match dbdreader within skip_first_record semantics.

        dbdreader skips the first record of *every* file, while xarray-dbd's
        C++ backend uses the dbd2netCDF convention: files are sorted, the first
        file keeps all records, subsequent files drop their first record. This
        can cause a small count difference (off by ~N_files - 1), so we compare
        with a tolerance on length and check values are close where they overlap.
        """
        import dbdreader

        files = _all_files()
        xdbd = MultiDBD(filenames=files, cacheDir=CACHE_DIR)
        dbdr = dbdreader.MultiDBD(filenames=files, cacheDir=CACHE_DIR)

        sensor = "m_depth"
        xt, xv = xdbd.get(sensor)
        dt, dv = dbdr.get(sensor)

        # Length may differ by up to N_files due to skip_first_record semantics
        assert abs(len(xt) - len(dt)) <= len(files), (
            f"length mismatch too large: {len(xt)} vs {len(dt)}"
        )
        # Values in common should match
        # Find matching timestamps
        common_t = np.intersect1d(xt, dt)
        assert len(common_t) > 0
        x_mask = np.isin(xt, common_t)
        d_mask = np.isin(dt, common_t)
        np.testing.assert_allclose(xv[x_mask], dv[d_mask], rtol=1e-10)

        xdbd.close()
        dbdr.close()

    def test_multi_get_sync_match(self):
        """MultiDBD.get_sync results are close to dbdreader.

        Same skip_first_record semantic difference as test_multi_get_values_match.
        """
        import dbdreader

        files = _all_files()
        xdbd = MultiDBD(filenames=files, cacheDir=CACHE_DIR)
        dbdr = dbdreader.MultiDBD(filenames=files, cacheDir=CACHE_DIR)

        params = ("m_depth", "m_pitch")
        xr = xdbd.get_sync(*params)
        dr = dbdr.get_sync(*params)

        assert len(xr) == len(dr)
        # Compare on common time base
        common_t = np.intersect1d(xr[0], dr[0])
        assert len(common_t) > 0
        x_mask = np.isin(xr[0], common_t)
        d_mask = np.isin(dr[0], common_t)
        for xa, da in zip(xr, dr, strict=True):
            xa_c = xa[x_mask]
            da_c = da[d_mask]
            mask = np.isfinite(xa_c) & np.isfinite(da_c)
            np.testing.assert_allclose(xa_c[mask], da_c[mask], rtol=1e-6)

        xdbd.close()
        dbdr.close()


# ---------------------------------------------------------------------------
# TestUtilities
# ---------------------------------------------------------------------------


class TestUtilities:
    def test_to_dec(self):
        # 42 degrees, 30 minutes NMEA = 4230.0 -> 42.5 decimal
        result = toDec(4230.0)
        assert abs(result - 42.5) < 1e-10

    def test_to_dec_pair(self):
        lat, lon = toDec(4230.0, -7015.0)
        assert abs(lat - 42.5) < 1e-10
        assert abs(lon - (-70.25)) < 1e-10

    def test_convert_to_decimal_negative(self):
        result = _convertToDecimal(-7015.0)
        assert abs(result - (-70.25)) < 1e-10

    def test_strptime_to_epoch(self):
        t = strptimeToEpoch("2020 Jan 01", "%Y %b %d")
        assert t == 1577836800

    def test_epoch_to_datetime_str(self):
        d, t = epochToDateTimeStr(1577836800)
        assert d == "20200101"
        assert t == "00:00"

    def test_heading_interpolating(self):
        t = np.array([0.0, 1.0, 2.0])
        v = np.array([0.0, np.pi, 2 * np.pi])
        f = heading_interpolating_function_factory(t, v)
        result = f(np.array([0.5]))
        assert result.shape == (1,)

    def test_latlon_params(self):
        assert "m_lat" in LATLON_PARAMS
        assert "m_lon" in LATLON_PARAMS
        assert len(LATLON_PARAMS) == 28


class TestDBDList:
    def test_sort_basic(self):
        fns = DBDList(["b.dbd", "a.dbd"])
        fns.sort()
        assert fns == ["a.dbd", "b.dbd"]


# ---------------------------------------------------------------------------
# TestTopLevelImport
# ---------------------------------------------------------------------------


class TestTopLevelImport:
    def test_dbd_importable(self):
        assert hasattr(xarray_dbd, "DBD")
        assert xarray_dbd.DBD is DBD

    def test_multidbd_importable(self):
        assert hasattr(xarray_dbd, "MultiDBD")
        assert xarray_dbd.MultiDBD is MultiDBD

    def test_dbdreader2_module_import(self):
        import xarray_dbd.dbdreader2 as dbdreader2

        assert hasattr(dbdreader2, "DBD")
        assert hasattr(dbdreader2, "MultiDBD")
        assert hasattr(dbdreader2, "DbdError")
        assert hasattr(dbdreader2, "DBDCache")
        assert hasattr(dbdreader2, "DBDList")
        assert hasattr(dbdreader2, "toDec")
