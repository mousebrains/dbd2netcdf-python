"""Cache directory manager compatible with dbdreader.DBDCache."""

from __future__ import annotations

import os
import sys

from ._errors import DBD_ERROR_CACHEDIR_NOT_FOUND, DbdError


class DBDCache:
    """Manages the default cache directory for sensor definition files.

    On linux:   ``$HOME/.local/share/dbdreader``
    On other:   ``$HOME/.dbdreader``

    Use ``DBDCache.set_cachedir(path)`` to override the default.
    """

    CACHEDIR: str | None = None

    def __init__(self, cachedir=None):
        if cachedir is None:
            if DBDCache.CACHEDIR is None:
                home = os.path.expanduser("~")
                if sys.platform == "linux":
                    cachedir = os.path.join(home, ".local/share/dbdreader")
                else:
                    cachedir = os.path.join(home, ".dbdreader")
                DBDCache.set_cachedir(cachedir, force_makedirs=True)
        else:
            DBDCache.set_cachedir(cachedir, force_makedirs=False)

    @classmethod
    def set_cachedir(cls, path, force_makedirs=False):
        """Set the cache directory path.

        Parameters
        ----------
        path : str
            Path to cache directory.
        force_makedirs : bool
            If True, create the directory if it doesn't exist.
        """
        if not os.path.exists(path):
            if force_makedirs:
                os.makedirs(path)
            else:
                raise DbdError(DBD_ERROR_CACHEDIR_NOT_FOUND)
        DBDCache.CACHEDIR = path
