"""dbdreader2 — drop-in replacement for dbdreader backed by xarray-dbd's C++ parser.

Usage::

    import xarray_dbd.dbdreader2 as dbdreader

    dbd = dbdreader.DBD("file.dbd", cacheDir="/path/to/cache")
    t, depth = dbd.get("m_depth")
"""

from __future__ import annotations

from ._cache import DBDCache
from ._core import DBD, MultiDBD
from ._errors import (
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
    DbdError,
)
from ._list import DBDList, DBDPatternSelect
from ._util import (
    LATLON_PARAMS,
    _convertToDecimal,
    epochToDateTimeStr,
    heading_interpolating_function_factory,
    strptimeToEpoch,
    toDec,
)

# Initialize default cache directory (matches dbdreader behavior)
DBDCache()

__all__ = [
    "DBD",
    "DBDCache",
    "DBDList",
    "DBDPatternSelect",
    "_convertToDecimal",
    "DBD_ERROR_ALL_FILES_BANNED",
    "DBD_ERROR_CACHE_NOT_FOUND",
    "DBD_ERROR_CACHEDIR_NOT_FOUND",
    "DBD_ERROR_INVALID_DBD_FILE",
    "DBD_ERROR_INVALID_ENCODING",
    "DBD_ERROR_INVALID_FILE_CRITERION_SPECIFIED",
    "DBD_ERROR_NO_DATA",
    "DBD_ERROR_NO_DATA_TO_INTERPOLATE",
    "DBD_ERROR_NO_DATA_TO_INTERPOLATE_TO",
    "DBD_ERROR_NO_FILE_CRITERIUM_SPECIFIED",
    "DBD_ERROR_NO_FILES_FOUND",
    "DBD_ERROR_NO_TIME_VARIABLE",
    "DBD_ERROR_NO_VALID_PARAMETERS",
    "DbdError",
    "LATLON_PARAMS",
    "MultiDBD",
    "epochToDateTimeStr",
    "heading_interpolating_function_factory",
    "strptimeToEpoch",
    "toDec",
]
