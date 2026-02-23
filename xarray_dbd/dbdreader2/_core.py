"""DBD and MultiDBD classes — drop-in replacements for dbdreader, backed by _dbd_cpp."""

from __future__ import annotations

import contextlib
import datetime
import glob
import logging
import os

import numpy

from xarray_dbd._dbd_cpp import read_dbd_file, read_dbd_files, scan_headers, scan_sensors

from ._cache import DBDCache
from ._errors import (
    DBD_ERROR_ALL_FILES_BANNED,
    DBD_ERROR_NO_DATA_TO_INTERPOLATE,
    DBD_ERROR_NO_DATA_TO_INTERPOLATE_TO,
    DBD_ERROR_NO_FILE_CRITERIUM_SPECIFIED,
    DBD_ERROR_NO_FILES_FOUND,
    DBD_ERROR_NO_TIME_VARIABLE,
    DBD_ERROR_NO_VALID_PARAMETERS,
    DbdError,
)
from ._list import DBDList
from ._util import LATLON_PARAMS, _convertToDecimal, strptimeToEpoch

logger = logging.getLogger(__name__)

# Fill sentinels matching C++ backend (ColumnData.H)
_FILL_INT8 = numpy.int8(-127)
_FILL_INT16 = numpy.int16(-32768)


def _is_fill(v: numpy.ndarray) -> numpy.ndarray:
    """Return boolean mask where values are fill/NaN."""
    if numpy.issubdtype(v.dtype, numpy.integer):
        if v.dtype == numpy.int8:
            return v == _FILL_INT8
        if v.dtype == numpy.int16:
            return v == _FILL_INT16
        return numpy.zeros(len(v), dtype=bool)
    return ~numpy.isfinite(v)


def _filter_latlon(t, v, param, discard_bad):
    """Apply NMEA lat/lon range filter if *param* is a lat/lon sensor."""
    if param not in LATLON_PARAMS:
        return t, v
    if discard_bad:
        limit = 9000 if "lat" in param else 18000
        cond = (v >= -limit) & (v <= limit)
        t = t[cond]
        v = v[cond]
    return t, v


class DBD:
    """Read a single DBD file with a dbdreader-compatible interface.

    Parameters
    ----------
    filename : str
        Path to a DBD/EBD/SBD/… file.
    cacheDir : str or None
        Sensor-cache directory. Defaults to ``DBDCache.CACHEDIR``.
    skip_initial_line : bool
        If True (default), skip the first data record.
    preload : list of str or None
        Sensor names to load alongside the first ``get()`` call.
    """

    SKIP_INITIAL_LINE = True

    def __init__(self, filename, cacheDir=None, skip_initial_line=True, preload=None):  # noqa: N803
        self.filename = filename
        self.skip_initial_line = skip_initial_line

        if cacheDir is None:
            self.cacheDir = DBDCache.CACHEDIR
        else:
            self.cacheDir = cacheDir

        cache = self.cacheDir or ""

        # Lightweight metadata-only scan — no data records read
        sensor_result = scan_sensors([filename], cache_dir=cache)
        names = sensor_result["sensor_names"]
        units = sensor_result["sensor_units"]
        self.parameterNames = list(names)
        self.parameterUnits = dict(zip(names, units, strict=True))

        hdr_result = scan_headers([filename])
        self.headerInfo: dict[str, str] = {}
        if hdr_result["filenames"]:
            self.headerInfo["mission_name"] = hdr_result["mission_names"][0]
            self.headerInfo["sensor_list_crc"] = hdr_result["sensor_list_crcs"][0]
            self.headerInfo["fileopen_time"] = hdr_result["fileopen_times"][0]

        self.cacheFound = bool(self.cacheDir and os.path.isdir(self.cacheDir))
        self.cacheID = self.headerInfo.get("sensor_list_crc", "").lower()

        self.timeVariable = self._set_timeVariable()

        # Lazy state — no columns loaded yet
        self._columns: dict[str, numpy.ndarray] = {}
        self._loaded_params: set[str] = set()
        self._preload: set[str] = set(preload) if preload else set()
        self._closed = False

    # -- lazy loading ------------------------------------------------------------

    def _ensure_loaded(self, params):
        """Load *params* (plus time variable and preload set) if not already in memory."""
        to_load = set(params) | {self.timeVariable}
        if self._preload:
            to_load |= self._preload
            self._preload = set()
        needed = to_load - self._loaded_params
        if not needed:
            return
        result = read_dbd_file(
            self.filename,
            cache_dir=self.cacheDir or "",
            to_keep=sorted(needed),
            skip_first_record=self.skip_initial_line,
        )
        # First load: upgrade headerInfo to the full header dict
        if not self._loaded_params:
            self.headerInfo = result["header"]
        names = result["sensor_names"]
        for i, name in enumerate(names):
            if name:  # sparse result: non-empty name means kept column
                self._columns[name] = numpy.asarray(result["columns"][i])
        self._loaded_params |= {n for n in names if n}

    # -- public methods ----------------------------------------------------------

    def get_mission_name(self):
        """Return the mission name (lowercase)."""
        return self.headerInfo.get("mission_name", "").lower()

    def get_fileopen_time(self):
        """Return file open time as seconds since epoch (UTC)."""
        datestr = self.headerInfo.get("fileopen_time", "").replace("_", " ")
        fmt = "%a %b %d %H:%M:%S %Y"
        return strptimeToEpoch(datestr, fmt)

    def has_parameter(self, parameter):
        """Return True if *parameter* is available in this file."""
        return parameter in self.parameterNames

    def close(self):
        """Release stored data."""
        self._closed = True
        self._columns = {}
        self._loaded_params = set()
        self.parameterNames = []
        self.parameterUnits = {}

    def get(
        self,
        *parameters,
        decimalLatLon=True,  # noqa: N803
        discardBadLatLon=True,  # noqa: N803
        return_nans=False,
        max_values_to_read=-1,
        check_for_invalid_parameters=True,
    ):
        """Return ``(t, v)`` per parameter, or a single tuple for one parameter.

        Parameters
        ----------
        *parameters : str
            Sensor names.
        decimalLatLon : bool
            Convert NMEA lat/lon to decimal degrees.
        discardBadLatLon : bool
            Filter out bogus lat/lon values.
        return_nans : bool
            If True, include fill/NaN rows.
        max_values_to_read : int
            Limit values read (only valid for single parameter).
        check_for_invalid_parameters : bool
            Raise on unknown sensor names.
        """
        if self._closed:
            raise DbdError(DBD_ERROR_NO_TIME_VARIABLE, "DBD object is closed")

        if max_values_to_read > 0 and len(parameters) != 1:
            raise ValueError(
                "Limiting the values to be read for multiple parameters "
                "potentially yields undefined behaviour.\n"
            )

        self._ensure_loaded(parameters)

        if check_for_invalid_parameters:
            invalid = [p for p in parameters if p not in self._columns]
            if invalid:
                if len(invalid) == 1:
                    mesg = (
                        f"Parameter {invalid[0]} is an unknown glider sensor name. "
                        f"({self.filename})"
                    )
                else:
                    mesg = (
                        f"Parameters {{{','.join(invalid)}}} are unknown glider sensor "
                        f"names. ({self.filename})"
                    )
                raise DbdError(value=DBD_ERROR_NO_VALID_PARAMETERS, mesg=mesg, data=invalid)

        if self.timeVariable not in self._columns:
            raise DbdError(DBD_ERROR_NO_TIME_VARIABLE)

        t_all = self._columns[self.timeVariable].astype(numpy.float64)
        results = []
        for p in parameters:
            t, v = self._extract_param(t_all, p, return_nans, decimalLatLon, discardBadLatLon)
            if max_values_to_read > 0:
                t = t[:max_values_to_read]
                v = v[:max_values_to_read]
            results.append((t, v))

        if len(parameters) == 1:
            return results[0]
        return results

    def get_list(
        self,
        *parameters,
        decimalLatLon=True,  # noqa: N803
        discardBadLatLon=True,  # noqa: N803
        return_nans=False,
    ):
        """Deprecated wrapper around :meth:`get`."""
        logger.info(
            "get_list has been deprecated in version 0.4.0 and may be removed "
            "in the future. Use get instead."
        )
        return self.get(
            *parameters,
            decimalLatLon=decimalLatLon,
            discardBadLatLon=discardBadLatLon,
            return_nans=return_nans,
        )

    def get_xy(
        self,
        parameter_x,
        parameter_y,
        decimalLatLon=True,  # noqa: N803
        discardBadLatLon=True,  # noqa: N803
    ):
        """Return (x_values, y_values) with y interpolated onto x's time base."""
        _, x, y = self._get_sync(
            parameter_x,
            parameter_y,
            decimalLatLon=decimalLatLon,
            discardBadLatLon=discardBadLatLon,
        )
        return x, y

    def get_sync(
        self,
        *sync_parameters,
        decimalLatLon=True,  # noqa: N803
        discardBadLatLon=True,  # noqa: N803
    ):
        """Return (t, v0, v1, ...) with all values on the first parameter's time base.

        At least two parameters required.
        """
        if len(sync_parameters) < 2:
            raise ValueError("Expect at least two parameters.")
        if len(sync_parameters) == 2 and isinstance(sync_parameters[1], (list, tuple)):
            sync_parameters = [sync_parameters[0]] + list(sync_parameters[1])
        return self._get_sync(
            *sync_parameters,
            decimalLatLon=decimalLatLon,
            discardBadLatLon=discardBadLatLon,
        )

    # -- private helpers ---------------------------------------------------------

    def _set_timeVariable(self):  # noqa: N802
        if "m_present_time" in self.parameterNames:
            return "m_present_time"
        return "sci_m_present_time"

    def _extract_param(self, t_all, param, return_nans, decimal_ll, discard_bad_ll):
        """Extract (t, v) for a single parameter, applying filters."""
        if param not in self._columns:
            empty = numpy.array([], dtype=numpy.float64)
            return empty, empty

        v = self._columns[param].copy()
        t = t_all.copy()

        is_int = numpy.issubdtype(v.dtype, numpy.integer)

        if return_nans:
            # Replace fill values with NaN for float output
            if is_int:
                v = v.astype(numpy.float64)
                v[_is_fill(self._columns[param])] = numpy.nan
            else:
                v = v.astype(numpy.float64)
            return t, v

        # Filter out fill/NaN values
        fill_mask = _is_fill(v)
        t_finite = numpy.isfinite(t)
        mask = t_finite & ~fill_mask
        t = t[mask]
        v = v[mask].astype(numpy.float64)

        # Lat/lon filtering and conversion
        if param in LATLON_PARAMS:
            t, v = _filter_latlon(t, v, param, discard_bad_ll)
            if decimal_ll:
                v = _convertToDecimal(v)

        return t, v

    def _get_sync(self, *params, decimalLatLon=True, discardBadLatLon=True):  # noqa: N803
        """Internal sync implementation."""
        if self._closed:
            raise DbdError(DBD_ERROR_NO_TIME_VARIABLE, "DBD object is closed")
        self._ensure_loaded(params)
        t_all = self._columns[self.timeVariable].astype(numpy.float64)
        tv_pairs = [
            self._extract_param(t_all, p, False, decimalLatLon, discardBadLatLon) for p in params
        ]

        t_ref = tv_pairs[0][0]
        if t_ref.shape[0] == 0:
            raise DbdError(DBD_ERROR_NO_DATA_TO_INTERPOLATE_TO)

        r = [t_ref, tv_pairs[0][1]]
        for i, (_t, _v) in enumerate(tv_pairs[1:], 1):
            try:
                r.append(numpy.interp(t_ref, _t, _v, left=numpy.nan, right=numpy.nan))
            except ValueError:
                r.append(t_ref * numpy.nan)
                logger.info("No valid data to interpolate for '%s'.", params[i])

        return tuple(r)


class MultiDBD:
    """Read multiple DBD files with a dbdreader-compatible interface.

    Parameters
    ----------
    filenames : list of str or str or None
        List of file paths, or a single string (treated as pattern if
        *pattern* is None).
    pattern : str or None
        Glob pattern for finding files.
    cacheDir : str or None
        Sensor-cache directory.
    complemented_files_only : bool
        Keep only files that have both eng and sci counterparts.
    complement_files : bool
        Auto-pair eng/sci files by swapping extension.
    banned_missions : list of str
        Mission names to exclude.
    missions : list of str
        Only include these mission names.
    max_files : int or None
        Limit files (positive = first N, negative = last N).
    skip_initial_line : bool
        Skip first data record per file.
    preload : list of str or None
        Sensor names to load alongside the first ``get()`` call.
    """

    def __init__(
        self,
        filenames=None,
        pattern=None,
        cacheDir=None,  # noqa: N803
        complemented_files_only=False,
        complement_files=False,
        banned_missions=(),
        missions=(),
        max_files=None,
        skip_initial_line=True,
        preload=None,
    ):
        self._closed = False
        self._preload: set[str] = set(preload) if preload else set()
        self.banned_missions = list(banned_missions)
        self.missions = list(missions)
        self.skip_initial_line = skip_initial_line

        if cacheDir is None:
            cacheDir = DBDCache.CACHEDIR  # noqa: N806
        self.cacheDir = cacheDir

        # Resolve file list
        if not filenames and not pattern:
            raise DbdError(DBD_ERROR_NO_FILE_CRITERIUM_SPECIFIED)

        fns = DBDList()
        # Handle common mistake: string passed as filenames
        if isinstance(filenames, str):
            if pattern is None:
                pattern = filenames
                filenames = None
            else:
                raise DbdError(
                    DBD_ERROR_NO_FILE_CRITERIUM_SPECIFIED,
                    "I got a string for <filenames> (no list), and a string for <pattern>.",
                )
        if filenames:
            fns += filenames
        if pattern:
            fns += glob.glob(pattern)
        if len(fns) == 0:
            raise DbdError(DBD_ERROR_NO_FILES_FOUND)
        fns.sort()

        if max_files and max_files > 0:
            fns = DBDList(fns[:max_files])
        elif max_files and max_files < 0:
            fns = DBDList(fns[max_files:])

        if complement_files:
            self._add_paired_filenames(fns)

        if complemented_files_only:
            self._prune_unmatched(fns)

        # Use scan_headers for mission filtering + fileopen_times
        hdr_result = scan_headers(
            list(fns),
            skip_missions=self.banned_missions if self.banned_missions else [],
            keep_missions=self.missions if self.missions else [],
        )
        valid_set = set(hdr_result["filenames"])
        self.mission_list = sorted(set(hdr_result["mission_names"]))
        # Build {filename: mission_name} mapping for later use
        self._file_missions = dict(
            zip(hdr_result["filenames"], hdr_result["mission_names"], strict=True)
        )

        # Build {filename: open_time_epoch} from scan_headers fileopen_times
        self._file_open_times: dict[str, float] = {}
        for fn, time_str in zip(hdr_result["filenames"], hdr_result["fileopen_times"], strict=True):
            datestr = time_str.replace("_", " ")
            fmt = "%a %b %d %H:%M:%S %Y"
            with contextlib.suppress(ValueError, OverflowError):
                self._file_open_times[fn] = strptimeToEpoch(datestr, fmt)

        # Filter to valid files only, preserving order
        fns = DBDList(f for f in fns if f in valid_set)
        fns.sort()
        self.filenames = list(fns)

        if not self.filenames:
            raise DbdError(DBD_ERROR_ALL_FILES_BANNED)

        # Partition into eng/sci and create lightweight DBD objects
        eng_dbds: list[DBD] = []
        sci_dbds: list[DBD] = []
        for f in self.filenames:
            dbd = DBD(f, cacheDir=self.cacheDir, skip_initial_line=skip_initial_line)
            if self.isScienceDataFile(f):
                sci_dbds.append(dbd)
            else:
                eng_dbds.append(dbd)
        self.dbds: dict[str, list[DBD]] = {"eng": eng_dbds, "sci": sci_dbds}

        # Lazy state — metadata only, no data columns loaded
        self._eng_columns: dict[str, numpy.ndarray] = {}
        self._sci_columns: dict[str, numpy.ndarray] = {}
        eng_files = [d.filename for d in eng_dbds]
        sci_files = [d.filename for d in sci_dbds]
        self._load(eng_files, sci_files)

        # Time limits
        self.time_limits_dataset: tuple = (None, None)
        self.time_limits: list = [None, None]
        self._init_time_limits()

    # -- public methods ----------------------------------------------------------

    def get(
        self,
        *parameters,
        decimalLatLon=True,  # noqa: N803
        discardBadLatLon=True,  # noqa: N803
        return_nans=False,
        include_source=False,
        max_values_to_read=-1,
    ):
        """Return ``(t, v)`` per parameter."""
        self._check_closed()

        if include_source:
            raise NotImplementedError("include_source is not yet supported in dbdreader2")

        if max_values_to_read > 0 and len(parameters) != 1:
            raise ValueError(
                "Limiting the values to be read for multiple parameters "
                "potentially yields undefined behaviour.\n"
            )

        self._ensure_loaded(parameters)

        # Validate parameters
        all_known = set(self.parameterNames.get("eng", [])) | set(
            self.parameterNames.get("sci", [])
        )
        invalid = [p for p in parameters if p not in all_known]
        if invalid:
            if len(invalid) == 1:
                mesg = f"Parameter {invalid[0]} is an unknown glider sensor name."
            else:
                mesg = f"Parameters {{{','.join(invalid)}}} are unknown glider sensor names."
            raise DbdError(value=DBD_ERROR_NO_VALID_PARAMETERS, mesg=mesg, data=invalid)

        results = []
        for p in parameters:
            t, v = self._extract(p, return_nans, decimalLatLon, discardBadLatLon)
            if max_values_to_read > 0:
                t = t[:max_values_to_read]
                v = v[:max_values_to_read]
            results.append((t, v))

        if len(parameters) == 1:
            return results[0]
        return results

    def get_sync(
        self,
        *parameters,
        decimalLatLon=True,  # noqa: N803
        discardBadLatLon=True,  # noqa: N803
        interpolating_function_factory=None,
    ):
        """Return (t, v0, v1, ...) interpolated onto the first parameter's time base."""
        self._check_closed()

        if len(parameters) < 2:
            raise ValueError("Expect at least two parameters.")
        if len(parameters) == 2 and isinstance(parameters[1], (list, tuple)):
            parameters = [parameters[0]] + list(parameters[1])

        tv = self.get(
            *parameters,
            decimalLatLon=decimalLatLon,
            discardBadLatLon=discardBadLatLon,
            return_nans=False,
        )
        # Normalise to list of tuples
        if len(parameters) == 1:
            tv = [tv]

        t = tv[0][0]
        r = []
        for i, (p, (_t, _v)) in enumerate(zip(parameters, tv, strict=True)):
            if i == 0:
                r.append(_t)
                r.append(_v)
            else:
                ifun_factory = self._resolve_ifun(p, interpolating_function_factory)
                if ifun_factory is None:
                    # Use numpy.interp directly
                    try:
                        r.append(numpy.interp(t, _t, _v, left=numpy.nan, right=numpy.nan))
                    except ValueError:
                        r.append(t * numpy.nan)
                        logger.info("No valid data to interpolate for '%s'.", p)
                else:
                    try:
                        ifun = ifun_factory(_t, _v)
                    except ValueError:
                        r.append(t * numpy.nan)
                        logger.info("No valid data to interpolate for '%s'.", p)
                    else:
                        r.append(ifun(t))

        return tuple(r)

    def get_xy(
        self,
        parameter_x,
        parameter_y,
        decimalLatLon=True,  # noqa: N803
        discardBadLatLon=True,  # noqa: N803
        interpolating_function_factory=None,
    ):
        """Return (x_values, y_values)."""
        _, x, y = self.get_sync(
            parameter_x,
            parameter_y,
            decimalLatLon=decimalLatLon,
            discardBadLatLon=discardBadLatLon,
            interpolating_function_factory=interpolating_function_factory,
        )
        return x, y

    def get_CTD_sync(  # noqa: N802
        self,
        *parameters,
        decimalLatLon=True,  # noqa: N803
        discardBadLatLon=True,  # noqa: N803
        interpolating_function_factory=None,
    ):
        """Return CTD + extra parameters, all sync'd to CTD timestamp.

        Returns (tctd, C, T, P, *extra_params).
        """
        ctd_type = self.determine_ctd_type()
        ctd_params = [
            f"sci_{ctd_type}_timestamp",
            "sci_water_cond",
            "sci_water_temp",
            "sci_water_pressure",
        ]
        offset = len(ctd_params) + 1  # +1 for m_present_time

        tmp = self.get_sync(
            *ctd_params,
            *parameters,
            decimalLatLon=decimalLatLon,
            discardBadLatLon=discardBadLatLon,
            interpolating_function_factory=interpolating_function_factory,
        )

        # Filter: timestamp > 1 and conductivity > 0
        tmp = numpy.compress(tmp[1] > 1, tmp, axis=1)
        condition = tmp[2] > 0

        if len(parameters):
            a = numpy.prod(tmp[offset:], axis=0)
            condition = condition & numpy.isfinite(a)

        if numpy.all(~condition):
            raise DbdError(DBD_ERROR_NO_DATA_TO_INTERPOLATE)

        # Ensure monotonicity in time
        dt = numpy.hstack(([1], numpy.diff(tmp[1])))
        condition = condition & (dt > 0)

        _, tctd, c, t_w, p_w, *v = numpy.compress(condition, tmp, axis=1)
        return tuple([tctd, c, t_w, p_w] + v)

    def determine_ctd_type(self):
        """Determine installed CTD type: ``"ctd41cp"`` or ``"rbrctd"``."""
        for ctd_type in ("ctd41cp", "rbrctd"):
            if self._has_ctd_installed(ctd_type):
                return ctd_type
        return "ctd41cp"

    def set_time_limits(self, minTimeUTC=None, maxTimeUTC=None):  # noqa: N803
        """Filter data by file open time. Triggers re-read of affected files."""
        if minTimeUTC:
            self.time_limits[0] = self._convert_seconds(minTimeUTC)
        if maxTimeUTC:
            self.time_limits[1] = self._convert_seconds(maxTimeUTC)
        self._apply_time_limits()

    def get_time_range(self, fmt="%d %b %Y %H:%M"):
        """Return formatted (start, end) of the selected time range."""
        return self._get_time_range(self.time_limits, fmt)

    def get_global_time_range(self, fmt="%d %b %Y %H:%M"):
        """Return formatted (start, end) of the entire dataset."""
        return self._get_time_range(self.time_limits_dataset, fmt)

    def has_parameter(self, parameter):
        """Return True if *parameter* is available."""
        return parameter in self.parameterNames.get(
            "sci", []
        ) or parameter in self.parameterNames.get("eng", [])

    def set_skip_initial_line(self, skip_initial_line):
        """Change skip_initial_line and trigger a re-read."""
        self.skip_initial_line = skip_initial_line
        eng_files = [d.filename for d in self.dbds["eng"]]
        sci_files = [d.filename for d in self.dbds["sci"]]
        self._load(eng_files, sci_files)

    def close(self):
        """Release all stored data."""
        self._closed = True
        for dbd in self.dbds.get("eng", []):
            dbd.close()
        for dbd in self.dbds.get("sci", []):
            dbd.close()
        self._eng_columns = {}
        self._sci_columns = {}
        self._loaded_eng_params = set()
        self._loaded_sci_params = set()
        self.parameterNames = {"eng": [], "sci": []}
        self.parameterUnits = {}

    @classmethod
    def isScienceDataFile(cls, fn):  # noqa: N802
        """Return True if *fn* is a science data file (ebd/tbd/nbd/ecd/tcd/ncd)."""
        fn_lower = fn.lower()
        return any(fn_lower.endswith(ext) for ext in ("ebd", "tbd", "nbd", "ecd", "tcd", "ncd"))

    # -- private helpers ---------------------------------------------------------

    def _check_closed(self):
        if self._closed:
            raise RuntimeError("MultiDBD object is closed")

    def _load(self, eng_files, sci_files):
        """Scan metadata for eng and sci file sets (no data read)."""
        cache = self.cacheDir or ""

        # Save previous loaded params for reload (set_skip_initial_line, time limits)
        prev_eng = set(self._loaded_eng_params) if hasattr(self, "_loaded_eng_params") else set()
        prev_sci = set(self._loaded_sci_params) if hasattr(self, "_loaded_sci_params") else set()

        self._eng_files = eng_files
        self._sci_files = sci_files
        self._eng_columns = {}
        self._sci_columns = {}
        self._eng_units: dict[str, str] = {}
        self._sci_units: dict[str, str] = {}
        self._loaded_eng_params: set[str] = set()
        self._loaded_sci_params: set[str] = set()
        self._eng_param_names: list[str] = []
        self._sci_param_names: list[str] = []

        if eng_files:
            result = scan_sensors(eng_files, cache_dir=cache)
            names = result["sensor_names"]
            units = result["sensor_units"]
            self._eng_param_names = list(names)
            self._eng_units = dict(zip(names, units, strict=True))

        if sci_files:
            result = scan_sensors(sci_files, cache_dir=cache)
            names = result["sensor_names"]
            units = result["sensor_units"]
            self._sci_param_names = list(names)
            self._sci_units = dict(zip(names, units, strict=True))

        self.parameterNames = {
            "eng": sorted(self._eng_param_names),
            "sci": sorted(self._sci_param_names),
        }
        self.parameterUnits = {**self._eng_units, **self._sci_units}

        # Reload previously-loaded params (for set_skip_initial_line / time limits)
        if prev_eng or prev_sci:
            self._ensure_loaded(prev_eng | prev_sci)

    def _ensure_loaded(self, params):
        """Load requested params (plus time variables and preload set) if not already in memory."""
        cache = self.cacheDir or ""
        skip = self.skip_initial_line
        eng_names_set = set(self._eng_param_names)
        sci_names_set = set(self._sci_param_names)

        to_load = set(params)
        if self._preload:
            to_load |= self._preload
            self._preload = set()

        # Partition requested params into eng/sci
        eng_requested = {p for p in to_load if p in eng_names_set}
        sci_requested = {p for p in to_load if p in sci_names_set}

        # Add time variables
        if eng_requested:
            for tv in ("m_present_time", "sci_m_present_time"):
                if tv in eng_names_set:
                    eng_requested.add(tv)
        if sci_requested:
            for tv in ("m_present_time", "sci_m_present_time"):
                if tv in sci_names_set:
                    sci_requested.add(tv)

        eng_needed = eng_requested - self._loaded_eng_params
        sci_needed = sci_requested - self._loaded_sci_params

        if eng_needed and self._eng_files:
            result = read_dbd_files(
                self._eng_files,
                cache_dir=cache,
                to_keep=sorted(eng_needed),
                skip_first_record=skip,
            )
            names = result["sensor_names"]
            for i, name in enumerate(names):
                self._eng_columns[name] = numpy.asarray(result["columns"][i])
            self._loaded_eng_params |= set(names)

        if sci_needed and self._sci_files:
            result = read_dbd_files(
                self._sci_files,
                cache_dir=cache,
                to_keep=sorted(sci_needed),
                skip_first_record=skip,
            )
            names = result["sensor_names"]
            for i, name in enumerate(names):
                self._sci_columns[name] = numpy.asarray(result["columns"][i])
            self._loaded_sci_params |= set(names)

    def _find_time_var(self, columns):
        """Return the time variable name present in *columns*."""
        for tv in ("m_present_time", "sci_m_present_time"):
            if tv in columns:
                return tv
        return None

    def _extract(self, param, return_nans, decimal_ll, discard_bad_ll):
        """Extract (t, v) for a param from the appropriate dataset."""
        if param in self._sci_columns:
            columns = self._sci_columns
        elif param in self._eng_columns:
            columns = self._eng_columns
        else:
            return numpy.array([], dtype=numpy.float64), numpy.array([], dtype=numpy.float64)

        tv_name = self._find_time_var(columns)
        if tv_name is None:
            raise DbdError(DBD_ERROR_NO_TIME_VARIABLE)

        t_all = columns[tv_name].astype(numpy.float64)
        v = columns[param]

        if return_nans:
            vf = v.astype(numpy.float64)
            vf[_is_fill(v)] = numpy.nan
            return t_all.copy(), vf

        fill_mask = _is_fill(v)
        t_finite = numpy.isfinite(t_all)
        mask = t_finite & ~fill_mask
        t = t_all[mask]
        v = v[mask].astype(numpy.float64)

        if param in LATLON_PARAMS:
            t, v = _filter_latlon(t, v, param, discard_bad_ll)
            if decimal_ll:
                v = _convertToDecimal(v)

        return t, v

    def _resolve_ifun(self, param, factory):
        """Resolve interpolating function factory for a parameter."""
        if factory is None:
            return None
        try:
            return factory[param]
        except KeyError:
            return None
        except TypeError:
            return factory

    def _has_ctd_installed(self, ctd_type):
        """Check whether the given CTD type has data."""
        param = f"sci_{ctd_type}_timestamp"
        if not self.has_parameter(param):
            return False
        try:
            t, v = self.get(param, max_values_to_read=15)
        except DbdError:
            return False
        return len(v) >= 15

    def _init_time_limits(self):
        """Compute global time range from file open times (no file I/O)."""
        times = [t for fn in self.filenames if (t := self._file_open_times.get(fn)) is not None]
        if times:
            self.time_limits_dataset = (min(times), max(times))
            self.time_limits = [self.time_limits_dataset[0], self.time_limits_dataset[1]]

    def _apply_time_limits(self):
        """Re-filter files based on current time limits, reload data (no file I/O for filtering)."""
        t_min = self.time_limits[0] or 0
        t_max = self.time_limits[1] or 1e10

        eng_files = []
        sci_files = []
        for fn in self.filenames:
            t_open = self._file_open_times.get(fn)
            if t_open is None or t_open < t_min or t_open > t_max:
                continue
            if self.isScienceDataFile(fn):
                sci_files.append(fn)
            else:
                eng_files.append(fn)

        # Update dbds to only contain DBD objects for files within time limits
        kept = set(eng_files + sci_files)
        self.dbds = {
            "eng": [d for d in self.dbds["eng"] if d.filename in kept],
            "sci": [d for d in self.dbds["sci"] if d.filename in kept],
        }
        self._load(eng_files, sci_files)

    def _convert_seconds(self, timestring):
        """Parse a time string in either short or long format."""
        t_epoch = None
        with contextlib.suppress(ValueError, OverflowError):
            t_epoch = strptimeToEpoch(timestring, "%d %b %Y")
        with contextlib.suppress(ValueError, OverflowError):
            t_epoch = strptimeToEpoch(timestring, "%d %b %Y %H:%M")
        if not t_epoch:
            raise ValueError(
                'Could not convert time string. Expect a format like "3 Mar" or "3 Mar 12:30".'
            )
        return t_epoch

    def _format_time(self, t, fmt):
        tmp = datetime.datetime.fromtimestamp(t, datetime.UTC)
        return tmp.strftime(fmt)

    def _get_time_range(self, time_limits, fmt):
        if fmt == "%s":
            return time_limits
        return [self._format_time(x, fmt) for x in time_limits]

    @staticmethod
    def _get_matching_fn(fn):
        """Get the complementary eng/sci filename for *fn*."""
        sci_extensions = {".ebd", ".tbd", ".nbd", ".ecd", ".tcd", ".ncd"}
        _, extension = os.path.splitext(fn)
        ext_chars = list(extension)
        if extension.lower() not in sci_extensions:
            ext_chars[1] = chr(ord(extension[1]) + 1)
        else:
            ext_chars[1] = chr(ord(extension[1]) - 1)
        matching_ext = "".join(ext_chars)
        return fn.replace(extension, matching_ext)

    def _add_paired_filenames(self, fns):
        """Add complementary eng/sci files that exist on disk."""
        to_add = []
        for fn in list(fns):
            mfn = self._get_matching_fn(fn)
            if os.path.exists(mfn) and mfn not in fns:
                to_add.append(mfn)
        fns += to_add

    def _prune_unmatched(self, fns):
        """Remove files that don't have an eng/sci counterpart."""
        fn_set = set(fns)
        to_remove = [fn for fn in fns if self._get_matching_fn(fn) not in fn_set]
        for fn in to_remove:
            fns.remove(fn)
