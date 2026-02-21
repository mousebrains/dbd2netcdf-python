"""
xarray-dbd: An efficient xarray backend for Dinkum Binary Data (DBD) files

This package provides an xarray backend engine for reading glider DBD files
directly without conversion to NetCDF, using a C++ parser via pybind11.
"""

from ._dbd_cpp import read_dbd_file, read_dbd_files
from .backend import (
    DBDBackendEntrypoint,
    open_dbd_dataset,
    open_multi_dbd_dataset,
)

from importlib.metadata import version

__version__ = version("xarray-dbd")
__all__ = [
    "DBDBackendEntrypoint",
    "read_dbd_file",
    "read_dbd_files",
    "open_dbd_dataset",
    "open_multi_dbd_dataset",
]
