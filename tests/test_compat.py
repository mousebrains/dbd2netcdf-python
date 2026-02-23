"""Tests for the dbdreader-compatible API layer (xarray_dbd.compat)."""

from __future__ import annotations

import numpy as np
import pytest
from conftest import CACHE_DIR, DBD_DIR, skip_no_data

import xarray_dbd
from xarray_dbd.compat import DBD, MultiDBD

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

    def test_get_missing_parameter(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        t, v = dbd.get("nonexistent_sensor")
        assert len(t) == 0
        assert len(v) == 0
        dbd.close()

    def test_get_integer_sensor(self):
        """Integer fill values (-127 / -32768) should be filtered out."""
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        names = dbd.parameterNames
        # Find an integer sensor (sensor_size 1 or 2)
        int_sensors = [n for n in names if dbd._ds[n].dtype in (np.int8, np.int16)]
        if not int_sensors:
            pytest.skip("No integer sensors in test file")
        t, v = dbd.get(int_sensors[0])
        if dbd._ds[int_sensors[0]].dtype == np.int8:
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
        with pytest.raises(ValueError, match="at least 2"):
            dbd.get_sync("m_depth")
        dbd.close()

    def test_get_sync_missing_second_param(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        result = dbd.get_sync("m_depth", "nonexistent_sensor")
        t, v0, v1 = result
        assert len(v1) == len(t)
        if len(t) > 0:
            assert np.all(np.isnan(v1))
        dbd.close()

    def test_get_mission_name(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        name = dbd.get_mission_name()
        assert isinstance(name, str)
        assert len(name) > 0
        dbd.close()

    def test_close_get_raises(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        dbd.close()
        with pytest.raises(RuntimeError, match="closed"):
            dbd.get("m_depth")

    def test_close_properties_empty(self):
        dbd = DBD(_single_file(), cacheDir=CACHE_DIR)
        dbd.close()
        assert dbd.parameterNames == []
        assert dbd.parameterUnits == {}
        assert not dbd.has_parameter("m_depth")
        assert dbd.get_mission_name() == ""

    def test_default_cache_dir(self):
        """When cacheDir is None, use <file_dir>/cache automatically."""
        dbd = DBD(_single_file())
        assert len(dbd.parameterNames) > 0
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
        with pytest.raises(ValueError, match="[Nn]o files"):
            MultiDBD(filenames=[])

    def test_no_args_raises(self):
        with pytest.raises(ValueError, match="filenames or pattern"):
            MultiDBD()

    def test_parameter_names_dict(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        pn = mdbd.parameterNames
        assert isinstance(pn, dict)
        assert "eng" in pn
        assert "sci" in pn
        assert isinstance(pn["eng"], list)
        assert isinstance(pn["sci"], list)
        # All sensors accounted for
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

    def test_has_parameter(self):
        mdbd = MultiDBD(filenames=_all_files(), cacheDir=CACHE_DIR)
        assert mdbd.has_parameter("m_present_time")
        assert not mdbd.has_parameter("no_such_sensor_xyz")
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
            # Both should be close; NaN positions should match
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

        # xarray-dbd may include time vars that dbdreader excludes, but
        # all dbdreader names should be present in xarray-dbd
        assert dnames.issubset(xnames), f"missing in xdbd: {dnames - xnames}"

        xdbd_dbd.close()
        dbdr_dbd.close()


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
