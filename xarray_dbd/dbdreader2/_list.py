"""Sorted file list and pattern-based file selection compatible with dbdreader."""

from __future__ import annotations

import glob
import os
import re

import numpy

from ._errors import DBD_ERROR_NO_FILES_FOUND, DbdError
from ._util import strptimeToEpoch


class DBDList(list):
    """List subclass that sorts Slocum DBD filenames chronologically."""

    REGEX = re.compile(r"-[0-9]+-[0-9]+-[0-9]+-[0-9]+\.[demnstDEMNST][bB][dD]")

    def __init__(self, *p):
        list.__init__(self, *p)

    def _keyFilename(self, key):  # noqa: N802
        match = DBDList.REGEX.search(key)
        if match:
            s, extension = os.path.splitext(match.group())
            number_fields = s.split("-")
            n = sum(
                int(i) * 10**j
                for i, j in zip(number_fields[1:], [8, 5, 3, 0], strict=True)
            )
            r = f"{key[:match.span()[0]]}-{n}{extension.lower()}"
        else:
            r = key.lower()
        return r

    def sort(self, cmp=None, key=None, reverse=False):  # noqa: ARG002
        """Sort filenames ensuring DBD files are in chronological order."""
        list.sort(self, key=self._keyFilename, reverse=reverse)


class DBDPatternSelect:
    """Select DBD files based on date criteria.

    Parameters
    ----------
    date_format : str
        Format for interpreting date strings.
    cacheDir : str or None
        Path to cache directory.
    """

    cache: dict = {}  # noqa: RUF012

    def __init__(self, date_format="%d %m %Y", cacheDir=None):  # noqa: N803
        self.set_date_format(date_format)
        self.cacheDir = cacheDir

    def set_date_format(self, date_format):
        """Set the date format for interpreting from_date/until_date."""
        self.date_format = date_format

    def get_date_format(self):
        """Return the current date format string."""
        return self.date_format

    def select(self, pattern=None, filenames=(), from_date=None, until_date=None):
        """Select filenames matching time criteria.

        Parameters
        ----------
        pattern : str or None
            Glob pattern.
        filenames : list of str
            Explicit file list.
        from_date, until_date : str or None
            Date bounds in the configured format.

        Returns
        -------
        DBDList of matching filenames.
        """
        all_filenames = self.get_filenames(pattern, filenames, self.cacheDir)

        if not from_date and not until_date:
            return all_filenames

        t0 = strptimeToEpoch(from_date, self.date_format) if from_date else 1
        t1 = strptimeToEpoch(until_date, self.date_format) if until_date else 1e11
        return self._select(all_filenames, t0, t1)

    def bins(self, pattern=None, filenames=None, binsize=86400, t_start=None, t_end=None):
        """Bin filenames into time windows.

        Returns
        -------
        list of (float, DBDList) — (bin_center, files_in_bin)
        """
        fns = self.get_filenames(pattern, filenames)
        if not fns:
            raise DbdError(
                DBD_ERROR_NO_FILES_FOUND, f"No files matched search pattern {pattern}."
            )
        if t_start is None:
            t_start = numpy.min(list(self.cache.keys()))
        if t_end is None:
            t_end = numpy.max(list(self.cache.keys()))
        bin_edges = numpy.arange(t_start, t_end + binsize, binsize)
        return [
            ((left + right) / 2, self._select(fns, left, right))
            for left, right in zip(bin_edges[:-1], bin_edges[1:], strict=True)
        ]

    def get_filenames(self, pattern, filenames, cacheDir=None):  # noqa: N803
        """Return sorted filenames and update the open-time cache."""
        if not pattern and not filenames:
            raise ValueError("Expected some pattern to search files for or file list.")
        if pattern:
            all_filenames = DBDList(glob.glob(pattern))
        elif filenames:
            all_filenames = DBDList(filenames)
        else:
            raise ValueError("Supply either pattern or filenames argument.")
        all_filenames.sort()
        self._update_cache(all_filenames, cacheDir)
        return all_filenames

    def _update_cache(self, fns, cacheDir):  # noqa: N803
        # Lazy import to avoid circular dependency
        from ._core import DBD

        cached_filenames = DBDList(self.cache.values())
        cached_filenames.sort()
        for fn in fns:
            if fn not in cached_filenames:
                dbd = DBD(fn, cacheDir)
                t_open = dbd.get_fileopen_time()
                self.cache[t_open] = fn

    def _select(self, all_fns, t0, t1):
        open_times = numpy.array(list(self.cache.keys()))
        open_times = numpy.sort(open_times)
        selected_times = open_times.compress(
            numpy.logical_and(open_times >= t0, open_times <= t1)
        )
        fns = {self.cache[t] for t in selected_times}.intersection(all_fns)
        fns = DBDList(fns)
        fns.sort()
        return fns
