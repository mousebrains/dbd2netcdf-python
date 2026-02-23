"""Utility functions compatible with dbdreader."""

from __future__ import annotations

import datetime
import time
from calendar import timegm
from functools import partial

import numpy

LATLON_PARAMS = [
    "m_lat",
    "m_lon",
    "c_wpt_lat",
    "c_wpt_lon",
    "x_last_wpt_lat",
    "x_last_wpt_lon",
    "m_gps_lat",
    "m_gps_lon",
    "u_lat_goto_l99",
    "u_lon_goto_l99",
    "m_last_gps_lat_1",
    "m_last_gps_lon_1",
    "m_last_gps_lat_2",
    "m_last_gps_lon_2",
    "m_last_gps_lat_3",
    "m_last_gps_lon_3",
    "m_last_gps_lat_4",
    "m_last_gps_lon_4",
    "m_gps_ignored_lat",
    "m_gps_ignored_lon",
    "m_gps_invalid_lat",
    "m_gps_invalid_lon",
    "m_gps_toofar_lat",
    "m_gps_toofar_lon",
    "xs_lat",
    "xs_lon",
    "s_ini_lat",
    "s_ini_lon",
]


def strptimeToEpoch(datestr, fmt):  # noqa: N802
    """Convert a date string to seconds since epoch (UTC)."""
    ts = time.strptime(datestr, fmt)
    return timegm(ts)


def epochToDateTimeStr(seconds, dateformat="%Y%m%d", timeformat="%H:%M"):  # noqa: N802
    """Convert seconds since epoch to (datestr, timestr) tuple."""
    d = datetime.datetime.fromtimestamp(seconds, datetime.timezone.utc)
    return d.strftime(dateformat), d.strftime(timeformat)


def _convertToDecimal(x):  # noqa: N802
    """Convert NMEA latitude/longitude to decimal degrees."""
    sign = numpy.sign(x)
    x_abs = numpy.abs(x)
    degrees = numpy.floor(x_abs / 100.0)
    minutes = x_abs - degrees * 100
    return (degrees + minutes / 60.0) * sign


def toDec(x, y=None):  # noqa: N802
    """NMEA to decimal degree converter.

    Parameters
    ----------
    x : float or array
        Latitude or longitude in NMEA format.
    y : float or array, optional
        Second coordinate in NMEA format.

    Returns
    -------
    float or tuple of floats
    """
    if y is not None:
        return _convertToDecimal(x), _convertToDecimal(y)
    return _convertToDecimal(x)


def heading_interpolating_function_factory(t, v):
    """Interpolating function factory for heading (handles 0/2pi wraparound).

    Returns a function f(t) that interpolates heading values by decomposing
    into cos/sin components.
    """
    x = numpy.cos(v)
    y = numpy.sin(v)
    xi = partial(numpy.interp, xp=t, fp=x, left=numpy.nan, right=numpy.nan)
    yi = partial(numpy.interp, xp=t, fp=y, left=numpy.nan, right=numpy.nan)
    return lambda _t: numpy.arctan2(yi(_t), xi(_t)) % (2 * numpy.pi)
