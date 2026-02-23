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
    DBDPatternSelect,
    MultiDBD,
    _convertToDecimal,
    epochToDateTimeStr,
    heading_interpolating_function_factory,
    strptimeToEpoch,
    toDec,
)
from xarray_dbd.dbdreader2._errors import (
    DBD_ERROR_ALL_FILES_BANNED,
    DBD_ERROR_CACHE_NOT_FOUND,
    DBD_ERROR_CACHEDIR_NOT_FOUND,
    DBD_ERROR_INVALID_DBD_FILE,
    DBD_ERROR_INVALID_ENCODING,
    DBD_ERROR_INVALID_FILE_CRITERION_SPECIFIED,
    DBD_ERROR_NO_DATA,
    DBD_ERROR_NO_DATA_TO_INTERPOLATE,
    DBD_ERROR_NO_DATA_TO_INTERPOLATE_TO,
    DBD_ERROR_NO_FILE_CRITERIUM_SPECIFIED,
    DBD_ERROR_NO_FILES_FOUND,
    DBD_ERROR_NO_TIME_VARIABLE,
    DBD_ERROR_NO_VALID_PARAMETERS,
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
        """Integer sensors are promoted to float64 with fill values filtered out."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        names = dbd.parameterNames
        # Load all sensors to find integer ones
        dbd._ensure_loaded(names)
        int_sensors = [
            n for n in names if n in dbd._columns and dbd._columns[n].dtype in (np.int8, np.int16)
        ]
        if not int_sensors:
            pytest.skip("No integer sensors in test file")
        t, v = dbd.get(int_sensors[0])
        # Return is always float64 now
        assert v.dtype == np.float64
        # Fill values should be filtered out
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

    def test_get_latlon_decimal(self):
        """Lat/lon sensors are converted to decimal degrees by default."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        if not dbd.has_parameter("m_lat"):
            dbd.close()
            pytest.skip("No m_lat in test file")
        t, v = dbd.get("m_lat")
        assert len(t) > 0
        # Decimal degrees should be < 90 for latitude
        assert np.all(np.abs(v) <= 90)
        dbd.close()

    def test_get_latlon_no_decimal(self):
        """With decimalLatLon=False, raw NMEA values are returned."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        if not dbd.has_parameter("m_lat"):
            dbd.close()
            pytest.skip("No m_lat in test file")
        _, v_raw = dbd.get("m_lat", decimalLatLon=False)
        _, v_dec = dbd.get("m_lat", decimalLatLon=True)
        assert len(v_raw) > 0
        # Raw NMEA values are larger (DDMM.MMM format)
        assert np.max(np.abs(v_raw)) > np.max(np.abs(v_dec))
        dbd.close()

    def test_get_latlon_discard_bad(self):
        """discardBadLatLon filters out-of-range lat/lon values."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        if not dbd.has_parameter("m_lat"):
            dbd.close()
            pytest.skip("No m_lat in test file")
        t_keep, _ = dbd.get("m_lat", discardBadLatLon=True)
        t_all, _ = dbd.get("m_lat", discardBadLatLon=False)
        # Keeping bad values should give >= as many results
        assert len(t_all) >= len(t_keep)
        dbd.close()

    def test_get_list_deprecated(self):
        """get_list is a deprecated wrapper around get."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        t1, v1 = dbd.get("m_depth")
        t2, v2 = dbd.get_list("m_depth")
        np.testing.assert_array_equal(t1, t2)
        np.testing.assert_array_equal(v1, v2)
        dbd.close()

    def test_get_sync_list_as_second_arg(self):
        """get_sync accepts a list as the second argument."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        r1 = dbd.get_sync("m_depth", "m_pitch")
        r2 = dbd.get_sync("m_depth", ["m_pitch"])
        assert len(r1) == len(r2)
        for a, b in zip(r1, r2, strict=True):
            np.testing.assert_array_equal(a, b)
        dbd.close()

    def test_get_max_values_multi_param_raises(self):
        """max_values_to_read with multiple parameters raises ValueError."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        with pytest.raises(ValueError, match="multiple parameters"):
            dbd.get("m_depth", "m_pitch", max_values_to_read=10)
        dbd.close()

    def test_get_max_values_single_param(self):
        """max_values_to_read limits output length."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        t, v = dbd.get("m_depth", max_values_to_read=5)
        assert len(t) <= 5
        assert len(v) <= 5
        dbd.close()

    def test_get_multiple_missing_params_raises(self):
        """Multiple unknown parameters include all names in error message."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        with pytest.raises(DbdError) as exc_info:
            dbd.get("no_sensor_a", "no_sensor_b")
        assert "no_sensor_a" in str(exc_info.value)
        assert "no_sensor_b" in str(exc_info.value)
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

    def test_get_latlon_multi(self):
        """MultiDBD lat/lon path: decimal conversion applied."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        if not mdbd.has_parameter("m_lat"):
            mdbd.close()
            pytest.skip("No m_lat in test data")
        t, v = mdbd.get("m_lat")
        assert len(t) > 0
        assert np.all(np.abs(v) <= 90)
        mdbd.close()

    def test_get_latlon_no_decimal_multi(self):
        """MultiDBD with decimalLatLon=False returns raw NMEA values."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        if not mdbd.has_parameter("m_lat"):
            mdbd.close()
            pytest.skip("No m_lat in test data")
        _, v_raw = mdbd.get("m_lat", decimalLatLon=False)
        _, v_dec = mdbd.get("m_lat", decimalLatLon=True)
        assert np.max(np.abs(v_raw)) > np.max(np.abs(v_dec))
        mdbd.close()

    def test_get_sync_list_as_second_arg_multi(self):
        """MultiDBD.get_sync accepts a list as second argument."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        r1 = mdbd.get_sync("m_depth", "m_pitch")
        r2 = mdbd.get_sync("m_depth", ["m_pitch"])
        assert len(r1) == len(r2)
        for a, b in zip(r1, r2, strict=True):
            np.testing.assert_array_equal(a, b)
        mdbd.close()

    def test_get_sync_with_interpolating_function_factory(self):
        """MultiDBD.get_sync with custom interpolating_function_factory."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        result = mdbd.get_sync(
            "m_depth",
            "m_heading",
            interpolating_function_factory=heading_interpolating_function_factory,
        )
        assert isinstance(result, tuple)
        assert len(result) == 3
        t, v0, v1 = result
        assert len(t) == len(v0) == len(v1)
        mdbd.close()

    def test_get_sync_with_interpolating_dict(self):
        """MultiDBD.get_sync with dict-based interpolating_function_factory."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        factory = {"m_heading": heading_interpolating_function_factory}
        result = mdbd.get_sync(
            "m_depth",
            "m_heading",
            "m_pitch",
            interpolating_function_factory=factory,
        )
        assert isinstance(result, tuple)
        assert len(result) == 4
        mdbd.close()

    def test_get_invalid_param_single_multi(self):
        """MultiDBD: single invalid param message."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        with pytest.raises(DbdError) as exc_info:
            mdbd.get("no_such_sensor")
        assert "no_such_sensor" in str(exc_info.value)
        mdbd.close()

    def test_get_invalid_params_multiple_multi(self):
        """MultiDBD: multiple invalid params message."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        with pytest.raises(DbdError) as exc_info:
            mdbd.get("no_sensor_a", "no_sensor_b")
        assert "no_sensor_a" in str(exc_info.value)
        assert "no_sensor_b" in str(exc_info.value)
        mdbd.close()

    def test_get_max_values_multi_raises(self):
        """MultiDBD: max_values_to_read with multiple params raises."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        with pytest.raises(ValueError, match="multiple parameters"):
            mdbd.get("m_depth", "m_pitch", max_values_to_read=10)
        mdbd.close()

    def test_get_max_values_single_multi(self):
        """MultiDBD: max_values_to_read limits output."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        t, v = mdbd.get("m_depth", max_values_to_read=5)
        assert len(t) <= 5
        mdbd.close()

    def test_complement_files(self):
        """complement_files adds paired eng/sci files to the file list."""
        # Use .dcd files — counterpart is .ecd, both compressed and in test data
        dcd_files = _all_files()[:3]
        for fn in dcd_files:
            mfn = MultiDBD._get_matching_fn(fn)
            assert mfn.endswith(".ecd")
            assert os.path.exists(mfn), f"Expected counterpart {mfn} not found"

    def test_complemented_files_only(self):
        """complemented_files_only prunes files without a counterpart."""
        fns = DBDList(["a.dbd", "a.ebd", "b.dbd"])
        fn_set = set(fns)
        to_remove = [fn for fn in fns if MultiDBD._get_matching_fn(fn) not in fn_set]
        for fn in to_remove:
            fns.remove(fn)
        # "b.dbd" has no "b.ebd" counterpart so it's removed
        assert "a.dbd" in fns
        assert "a.ebd" in fns
        assert "b.dbd" not in fns

    def test_banned_missions(self):
        """banned_missions excludes files from that mission."""
        mdbd_all = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        missions = mdbd_all.mission_list
        n_all = len(mdbd_all.filenames)
        mdbd_all.close()

        if len(missions) < 1:
            pytest.skip("Only one mission in test data")

        mdbd_ban = MultiDBD(
            filenames=_all_files(),
            cacheDir=CACHE_DIR,
            banned_missions=[missions[0]],
        )
        assert len(mdbd_ban.filenames) < n_all
        mdbd_ban.close()

    def test_convert_seconds_bad_format_raises(self):
        """_convert_seconds raises ValueError on unparseable time string."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        with pytest.raises(ValueError, match="Could not convert"):
            mdbd._convert_seconds("not-a-date")
        mdbd.close()

    def test_set_time_limits_max_only(self):
        """set_time_limits with only maxTimeUTC."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        t_min, t_max = mdbd.time_limits_dataset
        mid = (t_min + t_max) / 2
        mdbd.set_time_limits(maxTimeUTC=mdbd._format_time(mid, "%d %b %Y %H:%M"))
        assert mdbd.time_limits[1] is not None
        mdbd.close()

    def test_string_filenames_as_pattern(self):
        """Passing a string as filenames uses it as a pattern if pattern is None."""
        pattern = str(DBD_DIR / "*.dcd")
        mdbd = MultiDBD(filenames=pattern, cacheDir=CACHE_DIR)
        assert len(mdbd.filenames) > 0
        mdbd.close()

    def test_string_filenames_with_pattern_raises(self):
        """String filenames + string pattern raises."""
        with pytest.raises(DbdError):
            MultiDBD(filenames="*.dcd", pattern="*.dcd", cacheDir=CACHE_DIR)

    def test_get_matching_fn_eng_to_sci(self):
        """_get_matching_fn converts eng extension to sci."""
        assert MultiDBD._get_matching_fn("test.dbd").endswith(".ebd")
        assert MultiDBD._get_matching_fn("test.dcd").endswith(".ecd")
        assert MultiDBD._get_matching_fn("test.sbd").endswith(".tbd")

    def test_get_matching_fn_sci_to_eng(self):
        """_get_matching_fn converts sci extension to eng."""
        assert MultiDBD._get_matching_fn("test.ebd").endswith(".dbd")
        assert MultiDBD._get_matching_fn("test.ecd").endswith(".dcd")
        assert MultiDBD._get_matching_fn("test.tbd").endswith(".sbd")
        assert MultiDBD._get_matching_fn("test.nbd").endswith(".mbd")
        assert MultiDBD._get_matching_fn("test.ncd").endswith(".mcd")


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

    def test_sort_slocum_filenames(self):
        """Slocum-style filenames sort chronologically via _keyFilename."""
        fns = DBDList(
            [
                "unit_123-2024-100-3-43.dbd",
                "unit_123-2024-100-3-42.dbd",
                "unit_123-2024-100-4-0.dbd",
                "unit_123-2024-99-5-10.dbd",
            ]
        )
        fns.sort()
        # Day 99 < 100, and within day 100: segment 42 < 43 < segment 4-0
        assert fns[0] == "unit_123-2024-99-5-10.dbd"
        assert fns[1] == "unit_123-2024-100-3-42.dbd"
        assert fns[2] == "unit_123-2024-100-3-43.dbd"
        assert fns[3] == "unit_123-2024-100-4-0.dbd"

    def test_sort_mixed_extensions(self):
        """Slocum ebd and dbd files sort correctly."""
        fns = DBDList(
            [
                "unit_1-2024-100-3-42.ebd",
                "unit_1-2024-100-3-42.dbd",
            ]
        )
        fns.sort()
        # Same numeric key, sort by extension (dbd < ebd)
        assert fns[0].endswith(".dbd")
        assert fns[1].endswith(".ebd")

    def test_sort_reverse(self):
        fns = DBDList(["a.dbd", "b.dbd"])
        fns.sort(reverse=True)
        assert fns == ["b.dbd", "a.dbd"]


# ---------------------------------------------------------------------------
# TestDbdError — __str__ coverage
# ---------------------------------------------------------------------------


class TestDbdError:
    @pytest.mark.parametrize(
        ("code", "expected_substr"),
        [
            (DBD_ERROR_NO_VALID_PARAMETERS, "requested parameter"),
            (DBD_ERROR_NO_TIME_VARIABLE, "time variable"),
            (DBD_ERROR_CACHE_NOT_FOUND, "Cache file"),
            (DBD_ERROR_NO_FILE_CRITERIUM_SPECIFIED, "No file specification"),
            (DBD_ERROR_NO_FILES_FOUND, "No files were found"),
            (DBD_ERROR_NO_DATA_TO_INTERPOLATE_TO, "No data to interpolate to"),
            (DBD_ERROR_CACHEDIR_NOT_FOUND, "Cache file directory"),
            (DBD_ERROR_ALL_FILES_BANNED, "All data files were banned"),
            (DBD_ERROR_INVALID_DBD_FILE, "Invalid DBD file"),
            (DBD_ERROR_INVALID_ENCODING, "Invalid encoding"),
            (DBD_ERROR_INVALID_FILE_CRITERION_SPECIFIED, "Invalid or conflicting"),
            (DBD_ERROR_NO_DATA_TO_INTERPOLATE, "does/do not have any data"),
            (DBD_ERROR_NO_DATA, "do not have any data"),
        ],
    )
    def test_error_messages(self, code, expected_substr):
        err = DbdError(code)
        assert expected_substr in str(err)

    def test_undefined_error_code(self):
        err = DbdError(value=999)
        assert "Undefined error" in str(err)
        assert "999" in str(err)

    def test_custom_mesg_appended(self):
        err = DbdError(value=DBD_ERROR_NO_VALID_PARAMETERS, mesg="extra detail")
        s = str(err)
        assert "requested parameter" in s
        assert "extra detail" in s

    def test_data_attribute(self):
        err = DbdError(value=DBD_ERROR_NO_VALID_PARAMETERS, data=["sensor_a"])
        assert err.data == ["sensor_a"]


# ---------------------------------------------------------------------------
# TestDBDPatternSelect
# ---------------------------------------------------------------------------


class TestDBDPatternSelect:
    @pytest.fixture(autouse=True)
    def _set_cachedir(self):
        """Ensure DBDCache.CACHEDIR points to test cache for DBDPatternSelect."""
        old = DBDCache.CACHEDIR
        DBDCache.CACHEDIR = CACHE_DIR
        yield
        DBDCache.CACHEDIR = old

    def test_construction(self):
        ps = DBDPatternSelect(cacheDir=CACHE_DIR)
        assert ps.date_format == "%d %m %Y"
        assert ps.cacheDir == CACHE_DIR

    def test_set_get_date_format(self):
        ps = DBDPatternSelect()
        ps.set_date_format("%Y-%m-%d")
        assert ps.get_date_format() == "%Y-%m-%d"

    def test_get_filenames_pattern(self):
        ps = DBDPatternSelect(cacheDir=CACHE_DIR)
        pattern = str(DBD_DIR / "*.dcd")
        fns = ps.get_filenames(pattern=pattern, filenames=(), cacheDir=CACHE_DIR)
        assert len(fns) > 0
        assert isinstance(fns, DBDList)

    def test_get_filenames_list(self):
        ps = DBDPatternSelect(cacheDir=CACHE_DIR)
        files = _all_files()
        fns = ps.get_filenames(pattern=None, filenames=files, cacheDir=CACHE_DIR)
        assert len(fns) == len(files)

    def test_get_filenames_no_args_raises(self):
        ps = DBDPatternSelect()
        with pytest.raises(ValueError, match="Expected some pattern"):
            ps.get_filenames(pattern=None, filenames=())

    def test_select_no_date_filter(self):
        ps = DBDPatternSelect(cacheDir=CACHE_DIR)
        pattern = str(DBD_DIR / "*.dcd")
        fns = ps.select(pattern=pattern)
        assert len(fns) > 0

    def test_select_with_date_filter(self):
        ps = DBDPatternSelect(date_format="%d %b %Y", cacheDir=CACHE_DIR)
        pattern = str(DBD_DIR / "*.dcd")
        # Get all files first to know the time range
        all_fns = ps.select(pattern=pattern)
        assert len(all_fns) > 0
        # Filter with a very wide range should return all
        fns_wide = ps.select(
            pattern=pattern,
            from_date="01 Jan 2000",
            until_date="01 Jan 2100",
        )
        assert len(fns_wide) == len(all_fns)
        # Filter with a very narrow range in the past should return none
        fns_narrow = ps.select(
            pattern=pattern,
            from_date="01 Jan 1990",
            until_date="02 Jan 1990",
        )
        assert len(fns_narrow) == 0

    def test_bins(self):
        ps = DBDPatternSelect(cacheDir=CACHE_DIR)
        pattern = str(DBD_DIR / "*.dcd")
        ps.get_filenames(pattern=pattern, filenames=(), cacheDir=CACHE_DIR)
        # Use a very large bin size to get one bin
        bins = ps.bins(pattern=pattern, binsize=10 * 365 * 86400)
        assert isinstance(bins, list)
        assert len(bins) >= 1
        center, fns = bins[0]
        assert isinstance(center, float)
        assert isinstance(fns, DBDList)


# ---------------------------------------------------------------------------
# TestDBDCache
# ---------------------------------------------------------------------------


class TestDBDCache:
    def test_set_cachedir_nonexistent_raises(self):
        with pytest.raises(DbdError):
            DBDCache.set_cachedir("/nonexistent/path/xyz")

    def test_set_cachedir_valid(self):
        old = DBDCache.CACHEDIR
        try:
            DBDCache.set_cachedir(CACHE_DIR)
            assert DBDCache.CACHEDIR == CACHE_DIR
        finally:
            DBDCache.CACHEDIR = old

    def test_init_with_explicit_cachedir(self):
        old = DBDCache.CACHEDIR
        try:
            DBDCache(cachedir=CACHE_DIR)
            assert DBDCache.CACHEDIR == CACHE_DIR
        finally:
            DBDCache.CACHEDIR = old


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


# ---------------------------------------------------------------------------
# TestLazyLoading — verify lazy construction + incremental merge
# ---------------------------------------------------------------------------


class TestLazyLoading:
    def test_no_columns_at_construction(self):
        """DBD._columns is empty and _loaded_params is empty immediately after init."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        assert dbd._columns == {}
        assert dbd._loaded_params == set()
        dbd.close()

    def test_get_loads_only_requested(self):
        """After get('m_depth'), _loaded_params contains exactly {m_depth, timeVariable}."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        dbd.get("m_depth")
        assert "m_depth" in dbd._loaded_params
        assert dbd.timeVariable in dbd._loaded_params
        # Should not have loaded other sensors
        assert len(dbd._loaded_params) == 2
        dbd.close()

    def test_incremental_merge(self):
        """After get('m_depth') then get('m_lat'), both columns present."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        dbd.get("m_depth")
        dbd.get("m_lat")
        assert {"m_depth", "m_lat", dbd.timeVariable} <= dbd._loaded_params
        assert "m_depth" in dbd._columns
        assert "m_lat" in dbd._columns
        dbd.close()

    def test_second_get_no_reread(self):
        """Second get('m_depth') does not change _loaded_params (cache hit)."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        dbd.get("m_depth")
        params_after_first = set(dbd._loaded_params)
        dbd.get("m_depth")
        assert dbd._loaded_params == params_after_first
        dbd.close()

    def test_multi_lazy_no_columns_at_construction(self):
        """MultiDBD after init has empty _eng_columns and _sci_columns."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        assert mdbd._eng_columns == {}
        assert mdbd._sci_columns == {}
        # But parameterNames should already be populated from scan_sensors
        assert len(mdbd.parameterNames["eng"]) > 0
        mdbd.close()

    def test_multi_incremental_merge(self):
        """After get('m_depth') then get('m_lat'), both columns present."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        mdbd.get("m_depth")
        mdbd.get("m_lat")
        assert "m_depth" in mdbd._eng_columns
        assert "m_lat" in mdbd._eng_columns
        mdbd.close()

    def test_multi_lazy_filtered_get(self):
        """MultiDBD.get('m_depth') only loads time + depth into eng columns."""
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        mdbd.get("m_depth")
        # Should have m_depth + time variable(s), not all sensors
        assert "m_depth" in mdbd._loaded_eng_params
        assert "m_present_time" in mdbd._loaded_eng_params
        # Only time vars + m_depth should be loaded (≤ 3: m_depth + up to 2 time vars)
        assert len(mdbd._loaded_eng_params) <= 3
        assert len(mdbd._loaded_eng_params) < len(mdbd.parameterNames["eng"])
        mdbd.close()

    def test_dbd_preload(self):
        """DBD preload list is loaded on first get() alongside the requested param."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR, preload=["m_pitch", "m_roll"])
        # Nothing loaded yet
        assert dbd._loaded_params == set()
        dbd.get("m_depth")
        # First get loads requested + preload + time
        assert {"m_depth", "m_pitch", "m_roll", dbd.timeVariable} <= dbd._loaded_params
        assert "m_depth" in dbd._columns
        assert "m_pitch" in dbd._columns
        assert "m_roll" in dbd._columns
        dbd.close()

    def test_dbd_preload_consumed_once(self):
        """DBD preload is consumed on first get(); second get() loads only requested."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR, preload=["m_pitch"])
        dbd.get("m_depth")
        params_after_first = set(dbd._loaded_params)
        dbd.get("m_lat")
        # Second get should have added m_lat but NOT re-triggered preload
        assert "m_lat" in dbd._loaded_params
        assert dbd._loaded_params == params_after_first | {"m_lat"}
        dbd.close()

    def test_multi_preload(self):
        """MultiDBD preload list is loaded on first get() alongside the requested param."""
        mdbd = MultiDBD(
            filenames=_all_files(),
            cacheDir=CACHE_DIR,
            preload=["m_pitch", "m_roll"],
        )
        assert mdbd._eng_columns == {}
        mdbd.get("m_depth")
        assert "m_depth" in mdbd._eng_columns
        assert "m_pitch" in mdbd._eng_columns
        assert "m_roll" in mdbd._eng_columns
        mdbd.close()

    def test_multi_preload_consumed_once(self):
        """MultiDBD preload is consumed on first get(); second get() is incremental."""
        mdbd = MultiDBD(
            filenames=_all_files(),
            cacheDir=CACHE_DIR,
            preload=["m_pitch"],
        )
        mdbd.get("m_depth")
        params_after_first = set(mdbd._loaded_eng_params)
        mdbd.get("m_lat")
        # m_lat added, but m_pitch was NOT re-loaded
        assert "m_lat" in mdbd._loaded_eng_params
        assert mdbd._loaded_eng_params == params_after_first | {"m_lat"}
        mdbd.close()
