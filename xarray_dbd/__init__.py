"""
xarray-dbd: An efficient xarray backend for Dinkum Binary Data (DBD) files

This package provides an xarray backend engine for reading glider DBD files
directly without conversion to NetCDF, matching or exceeding the performance
of dbd2netCDF.
"""

from .backend import (
    DBDBackendEntrypoint,
    open_dbd_dataset,
    open_multi_dbd_dataset,
)
from .reader import DBDReader

__version__ = "0.1"
__all__ = [
    "DBDBackendEntrypoint",
    "DBDReader",
    "open_dbd_dataset",
    "open_multi_dbd_dataset",
]
