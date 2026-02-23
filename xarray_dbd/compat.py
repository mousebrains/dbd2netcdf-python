"""
dbdreader-compatible API for xarray-dbd.

Provides ``DBD`` and ``MultiDBD`` classes that mirror the
`dbdreader <https://pypi.org/project/dbdreader/>`_ interface, backed
by xarray-dbd's fast single-pass C++ reader.

Example
-------
>>> import xarray_dbd as xdbd
>>> dbd = xdbd.DBD("file.dcd", cacheDir="cache")
>>> t, depth = dbd.get("m_depth")
>>> dbd.close()
"""

from __future__ import annotations

import glob as _glob
from pathlib import Path
from typing import Any

import numpy as np

from .backend import open_dbd_dataset, open_multi_dbd_dataset

__all__ = ["DBD", "MultiDBD"]

# Time variable names in priority order
_TIME_VARS = ("m_present_time", "sci_m_present_time")


def _find_time_var(ds: Any) -> str | None:
    """Return the first available time variable name in *ds*."""
    for name in _TIME_VARS:
        if name in ds:
            return name
    return None


def _extract(ds: Any, parameter: str, time_var: str, return_nans: bool) -> tuple:
    """Extract ``(time, values)`` for a single parameter."""
    if parameter not in ds:
        return np.array([], dtype=np.float64), np.array([], dtype=np.float64)

    t = ds[time_var].values.astype(np.float64)
    v = ds[parameter].values

    # Integer sensors use sentinel fill values, not NaN
    is_int = np.issubdtype(v.dtype, np.integer)

    if not return_nans:
        if is_int:
            # Filter out integer fill values (-127 for int8, -32768 for int16)
            if v.dtype == np.int8:
                mask = (np.isfinite(t)) & (v != np.int8(-127))
            elif v.dtype == np.int16:
                mask = (np.isfinite(t)) & (v != np.int16(-32768))
            else:
                mask = np.isfinite(t)
            t = t[mask]
            v = v[mask]
        else:
            vf = v.astype(np.float64)
            mask = np.isfinite(t) & np.isfinite(vf)
            t = t[mask]
            v = vf[mask]
    else:
        v = v if is_int else v.astype(np.float64)

    return t, v


class DBD:
    """Read a single DBD file with a dbdreader-compatible interface.

    Parameters
    ----------
    filename : str or Path
        Path to a DBD/EBD/SBD/… file.
    cacheDir : str or Path or None
        Sensor-cache directory.  Defaults to ``<file_dir>/cache``.
    skip_initial_line : bool
        Corresponds to ``skip_first_record`` (default True).
    """

    def __init__(
        self,
        filename: str | Path,
        cacheDir: str | Path | None = None,  # noqa: N803
        skip_initial_line: bool = True,
    ):
        self._ds = open_dbd_dataset(
            filename,
            cache_dir=cacheDir,
            skip_first_record=skip_initial_line,
        )
        self._time_var = _find_time_var(self._ds)

    # -- properties ----------------------------------------------------------

    @property
    def parameterNames(self) -> list[str]:  # noqa: N802
        """List of all sensor names in the file."""
        if self._ds is None:
            return []
        return list(self._ds.data_vars)

    @property
    def parameterUnits(self) -> dict[str, str]:  # noqa: N802
        """Mapping of sensor name → unit string."""
        if self._ds is None:
            return {}
        return {name: var.attrs.get("units", "") for name, var in self._ds.data_vars.items()}

    # -- public methods ------------------------------------------------------

    def has_parameter(self, name: str) -> bool:
        """Return True if *name* is a known sensor."""
        if self._ds is None:
            return False
        return name in self._ds

    def get(self, *parameters: str, return_nans: bool = False) -> tuple | list[tuple]:
        """Extract time-series data for one or more parameters.

        Parameters
        ----------
        *parameters : str
            Sensor names.
        return_nans : bool
            If False (default), rows where time or value is NaN/fill are
            dropped.

        Returns
        -------
        ``(t, v)`` for a single parameter, or ``[(t1, v1), …]`` for
        multiple parameters.
        """
        if self._ds is None:
            raise RuntimeError("DBD object is closed")
        if not parameters:
            raise ValueError("At least one parameter name is required")

        time_var = self._time_var
        if time_var is None:
            empty = np.array([], dtype=np.float64)
            results = [(empty, empty) for _ in parameters]
            return results[0] if len(parameters) == 1 else results

        results = [_extract(self._ds, p, time_var, return_nans) for p in parameters]
        return results[0] if len(parameters) == 1 else results

    def get_sync(self, *parameters: str) -> tuple:
        """Return time-synchronised data for multiple parameters.

        The first parameter defines the time base; subsequent parameters
        are linearly interpolated onto that time base using
        ``numpy.interp``.

        Parameters
        ----------
        *parameters : str
            At least two sensor names.

        Returns
        -------
        Tuple ``(time, v0, v1, …)`` with all arrays the same length.
        """
        if self._ds is None:
            raise RuntimeError("DBD object is closed")
        if len(parameters) < 2:
            raise ValueError("get_sync requires at least 2 parameters")

        time_var = self._time_var
        if time_var is None:
            empty = np.array([], dtype=np.float64)
            return tuple(empty for _ in range(len(parameters) + 1))

        # Reference time from first parameter
        t_ref, v_ref = _extract(self._ds, parameters[0], time_var, return_nans=False)
        result: list[np.ndarray] = [t_ref, v_ref]

        for p in parameters[1:]:
            t_p, v_p = _extract(self._ds, p, time_var, return_nans=False)
            if len(t_p) == 0:
                result.append(np.full_like(t_ref, np.nan))
            else:
                result.append(np.interp(t_ref, t_p, v_p, left=np.nan, right=np.nan))

        return tuple(result)

    def get_mission_name(self) -> str:
        """Return the mission name from the file header."""
        if self._ds is None:
            return ""
        return str(self._ds.attrs.get("mission_name", ""))

    def close(self) -> None:
        """Release the underlying dataset."""
        self._ds = None
        self._time_var = None


class MultiDBD:
    """Read multiple DBD files with a dbdreader-compatible interface.

    Parameters
    ----------
    filenames : list of str or None
        Explicit list of file paths.
    pattern : str or None
        Glob pattern to find files (used if *filenames* is not given).
    cacheDir : str or Path or None
        Sensor-cache directory.
    missions : list of str or None
        Keep only files matching these mission names.
    banned_missions : list of str or None
        Skip files matching these mission names.
    skip_initial_line : bool
        Corresponds to ``skip_first_record`` (default True).
    """

    def __init__(
        self,
        filenames: list[str] | None = None,
        pattern: str | None = None,
        cacheDir: str | Path | None = None,  # noqa: N803
        missions: list[str] | None = None,
        banned_missions: list[str] | None = None,
        skip_initial_line: bool = True,
    ):
        if filenames is not None:
            file_list = [str(f) for f in filenames]
        elif pattern is not None:
            file_list = sorted(_glob.glob(pattern))
        else:
            raise ValueError("Either filenames or pattern must be provided")

        if not file_list:
            raise ValueError("No files to read")

        self._ds = open_multi_dbd_dataset(
            file_list,
            cache_dir=cacheDir,
            skip_first_record=skip_initial_line,
            keep_missions=missions,
            skip_missions=banned_missions,
        )
        self._time_var = _find_time_var(self._ds)

    # -- properties ----------------------------------------------------------

    @property
    def parameterNames(self) -> dict[str, list[str]]:  # noqa: N802
        """Sensor names split into ``{'eng': [...], 'sci': [...]}``.

        Sensors whose name starts with ``sci_`` are classified under
        ``'sci'``; all others under ``'eng'``.
        """
        if self._ds is None:
            return {"eng": [], "sci": []}
        eng: list[str] = []
        sci: list[str] = []
        for name in self._ds.data_vars:
            if name.startswith("sci_"):
                sci.append(name)
            else:
                eng.append(name)
        return {"eng": eng, "sci": sci}

    @property
    def parameterUnits(self) -> dict[str, str]:  # noqa: N802
        """Mapping of sensor name → unit string."""
        if self._ds is None:
            return {}
        return {name: var.attrs.get("units", "") for name, var in self._ds.data_vars.items()}

    # -- public methods ------------------------------------------------------

    def has_parameter(self, name: str) -> bool:
        """Return True if *name* is a known sensor."""
        if self._ds is None:
            return False
        return name in self._ds

    def get(self, *parameters: str, return_nans: bool = False) -> tuple | list[tuple]:
        """Extract time-series data for one or more parameters.

        See :meth:`DBD.get` for details.
        """
        if self._ds is None:
            raise RuntimeError("MultiDBD object is closed")
        if not parameters:
            raise ValueError("At least one parameter name is required")

        time_var = self._time_var
        if time_var is None:
            empty = np.array([], dtype=np.float64)
            results = [(empty, empty) for _ in parameters]
            return results[0] if len(parameters) == 1 else results

        results = [_extract(self._ds, p, time_var, return_nans) for p in parameters]
        return results[0] if len(parameters) == 1 else results

    def get_sync(self, *parameters: str) -> tuple:
        """Return time-synchronised data for multiple parameters.

        See :meth:`DBD.get_sync` for details.
        """
        if self._ds is None:
            raise RuntimeError("MultiDBD object is closed")
        if len(parameters) < 2:
            raise ValueError("get_sync requires at least 2 parameters")

        time_var = self._time_var
        if time_var is None:
            empty = np.array([], dtype=np.float64)
            return tuple(empty for _ in range(len(parameters) + 1))

        t_ref, v_ref = _extract(self._ds, parameters[0], time_var, return_nans=False)
        result: list[np.ndarray] = [t_ref, v_ref]

        for p in parameters[1:]:
            t_p, v_p = _extract(self._ds, p, time_var, return_nans=False)
            if len(t_p) == 0:
                result.append(np.full_like(t_ref, np.nan))
            else:
                result.append(np.interp(t_ref, t_p, v_p, left=np.nan, right=np.nan))

        return tuple(result)

    def close(self) -> None:
        """Release the underlying dataset."""
        self._ds = None
        self._time_var = None
