"""
Xarray backend engine for DBD files using C++ parser
"""

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
from xarray.backends import BackendEntrypoint

from ._dbd_cpp import read_dbd_file, read_dbd_files


class DBDDataStore:
    """Data store for DBD files using C++ backend"""

    def __init__(
        self,
        filename: str | Path,
        skip_first_record: bool = True,
        repair: bool = False,
        to_keep: list | None = None,
        criteria: list | None = None,
        cache_dir: str | Path | None = None,
    ):
        self.filename = Path(filename)

        # Determine cache directory
        if cache_dir is None:
            cache_dir = str(self.filename.parent / "cache")
        else:
            cache_dir = str(cache_dir)

        # Call C++ backend
        result = read_dbd_file(
            str(self.filename),
            cache_dir=cache_dir,
            to_keep=to_keep or [],
            criteria=criteria or [],
            skip_first_record=skip_first_record,
            repair=repair,
        )

        self._columns = list(result["columns"])
        self._sensor_names = list(result["sensor_names"])
        self._sensor_units = list(result["sensor_units"])
        self._sensor_sizes = list(result["sensor_sizes"])
        self._n_records = int(result["n_records"])
        self._header = dict(result["header"])

    def get_variables(self) -> dict[str, xr.Variable]:
        """Get xarray variables for all sensors"""
        variables: dict[str, xr.Variable] = {}
        dims = ("i",)

        for idx, name in enumerate(self._sensor_names):
            data = self._columns[idx]
            attrs = {
                "units": self._sensor_units[idx],
                "sensor_size": self._sensor_sizes[idx],
            }
            variables[name] = xr.Variable(dims, data, attrs=attrs)

        return variables

    def get_attrs(self) -> dict[str, Any]:
        """Get global attributes"""
        return {
            "mission_name": self._header.get("mission_name", ""),
            "fileopen_time": self._header.get("fileopen_time", ""),
            "encoding_version": self._header.get("encoding_version", ""),
            "full_filename": self._header.get("full_filename", ""),
            "sensor_list_crc": self._header.get("sensor_list_crc", ""),
            "source_file": str(self.filename),
        }

    def get_dimensions(self) -> dict[str, int]:
        """Get dimensions"""
        return {"i": self._n_records, "j": 1}


class DBDBackendEntrypoint(BackendEntrypoint):
    """Xarray backend entrypoint for DBD files"""

    description = "Backend for reading Dinkum Binary Data (DBD) files"
    url = "https://github.com/mousebrains/dbd2netcdf"

    def open_dataset(  # type: ignore[override]
        self,
        filename_or_obj: str | Path,
        *,
        drop_variables: tuple[str] | None = None,
        skip_first_record: bool = True,
        repair: bool = False,
        to_keep: list | None = None,
        criteria: list | None = None,
        cache_dir: str | Path | None = None,
    ) -> xr.Dataset:
        """Open a DBD file as an xarray Dataset"""
        filename = Path(filename_or_obj)

        store = DBDDataStore(
            filename,
            skip_first_record=skip_first_record,
            repair=repair,
            to_keep=to_keep,
            criteria=criteria,
            cache_dir=cache_dir,
        )

        vars_dict = store.get_variables()
        attrs_dict = store.get_attrs()

        if drop_variables:
            vars_dict = {k: v for k, v in vars_dict.items() if k not in drop_variables}

        return xr.Dataset(vars_dict, attrs=attrs_dict)

    def guess_can_open(self, filename_or_obj: str | Path) -> bool:  # type: ignore[override]
        """Guess if this backend can open the file"""
        try:
            filename = Path(filename_or_obj)
            ext = filename.suffix.lower()
            return ext in [".dbd", ".ebd", ".sbd", ".tbd", ".mbd", ".nbd",
                           ".dcd", ".ecd", ".scd", ".tcd", ".mcd", ".ncd"]
        except (TypeError, AttributeError):
            return False


def open_dbd_dataset(
    filename: str | Path,
    skip_first_record: bool = True,
    repair: bool = False,
    to_keep: list | None = None,
    criteria: list | None = None,
    drop_variables: list | None = None,
    cache_dir: str | Path | None = None,
) -> xr.Dataset:
    """Open a DBD file as an xarray Dataset"""
    return xr.open_dataset(
        filename,
        engine=DBDBackendEntrypoint,
        skip_first_record=skip_first_record,
        repair=repair,
        to_keep=to_keep,
        criteria=criteria,
        drop_variables=drop_variables,
        cache_dir=cache_dir,
    )


def open_multi_dbd_dataset(
    filenames: Iterable[str | Path],
    skip_first_record: bool = True,
    repair: bool = False,
    to_keep: list | None = None,
    criteria: list | None = None,
    skip_missions: list | None = None,
    keep_missions: list | None = None,
    cache_dir: str | Path | None = None,
) -> xr.Dataset:
    """Open multiple DBD files as a single xarray Dataset

    Uses C++ backend for sequential two-pass approach with SensorsMap
    for exact C++ parity.
    """
    file_list = [str(Path(f)) for f in filenames]

    if not file_list:
        return xr.Dataset()

    cache_str = str(cache_dir) if cache_dir else ""

    result = read_dbd_files(
        file_list,
        cache_dir=cache_str,
        to_keep=to_keep or [],
        criteria=criteria or [],
        skip_missions=skip_missions or [],
        keep_missions=keep_missions or [],
        skip_first_record=skip_first_record,
        repair=repair,
    )

    columns = list(result["columns"])
    sensor_names = list(result["sensor_names"])
    sensor_units = list(result["sensor_units"])
    n_records = int(result["n_records"])
    n_files = int(result["n_files"])

    if not columns:
        return xr.Dataset()

    # Create dataset
    dims = ("i",)
    data_vars = {}

    for idx, name in enumerate(sensor_names):
        attrs = {"units": sensor_units[idx]}
        data_vars[name] = xr.Variable(dims, columns[idx], attrs=attrs)

    attrs = {
        "n_files": n_files,
        "total_records": n_records,
    }

    ds = xr.Dataset(data_vars, attrs=attrs)

    # To match dbd2netCDF, we need a 'j' dimension with size 1
    ds = ds.assign_coords(j=xr.DataArray([0], dims=["j"]))
    ds = ds.drop_vars("j")

    return ds
