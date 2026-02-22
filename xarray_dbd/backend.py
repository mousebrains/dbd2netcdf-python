"""
Xarray backend engine for DBD files using C++ parser
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
from xarray.backends import BackendEntrypoint

from ._dbd_cpp import read_dbd_file, read_dbd_files, scan_sensors

logger = logging.getLogger(__name__)

__all__ = [
    "DBDDataStore",
    "DBDBackendEntrypoint",
    "open_dbd_dataset",
    "open_multi_dbd_dataset",
    "write_multi_dbd_netcdf",
]


class DBDDataStore:
    """Data store for reading a single DBD file using the C++ backend.

    Parameters
    ----------
    filename : str or Path
        Path to the DBD file.
    skip_first_record : bool
        If True, skip the first data record (for deduplication in multi-file reads).
    repair : bool
        If True, attempt to recover data from corrupted records.
    to_keep : list of str or None
        Sensor names to keep. If None or empty, all sensors are loaded.
    criteria : list of str or None
        Sensor names used for record selection criteria.
    cache_dir : str, Path, or None
        Directory for sensor cache files. Defaults to ``<file_dir>/cache``.
    """

    def __init__(
        self,
        filename: str | Path,
        skip_first_record: bool = True,
        repair: bool = False,
        to_keep: list[str] | None = None,
        criteria: list[str] | None = None,
        cache_dir: str | Path | None = None,
    ):
        self.filename = Path(filename)

        # Determine cache directory
        cache_dir = str(self.filename.parent / "cache") if cache_dir is None else str(cache_dir)

        # Call C++ backend
        try:
            result = read_dbd_file(
                str(self.filename),
                cache_dir=cache_dir,
                to_keep=to_keep or [],
                criteria=criteria or [],
                skip_first_record=skip_first_record,
                repair=repair,
            )
        except RuntimeError as e:
            raise OSError(f"Failed to read {self.filename}: {e}") from e

        required_keys = {
            "columns",
            "sensor_names",
            "sensor_units",
            "sensor_sizes",
            "n_records",
            "header",
        }
        missing = required_keys - result.keys()
        if missing:
            raise OSError(
                f"Incomplete result from C++ backend for {self.filename}: missing {missing}"
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
    """Xarray backend entrypoint for DBD files.

    Registers as the ``"dbd"`` engine for :func:`xarray.open_dataset`.
    Supports all Slocum glider binary formats (``.dbd``, ``.ebd``, ``.sbd``,
    ``.tbd``, ``.mbd``, ``.nbd``) and their compressed variants (``.dcd``, etc.).
    """

    description = "Backend for reading Dinkum Binary Data (DBD) files"
    url = "https://github.com/mousebrains/dbd2netcdf"

    def open_dataset(  # type: ignore[override]
        self,
        filename_or_obj: str | Path,
        *,
        drop_variables: tuple[str, ...] | None = None,
        skip_first_record: bool = True,
        repair: bool = False,
        to_keep: list[str] | None = None,
        criteria: list[str] | None = None,
        cache_dir: str | Path | None = None,
    ) -> xr.Dataset:
        """Open a DBD file as an xarray Dataset.

        Parameters
        ----------
        filename_or_obj : str or Path
            Path to the DBD file.
        drop_variables : tuple of str, optional
            Variable names to exclude from the returned Dataset.
        skip_first_record : bool
            Skip the first data record (default True).
        repair : bool
            Attempt to repair corrupted records (default False).
        to_keep : list of str, optional
            Sensor names to keep. If None, all sensors are loaded.
        criteria : list of str, optional
            Sensor names for record selection criteria.
        cache_dir : str, Path, or None
            Directory for sensor cache files.

        Returns
        -------
        xr.Dataset
        """
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
            drop_set = set(drop_variables)
            vars_dict = {k: v for k, v in vars_dict.items() if k not in drop_set}

        return xr.Dataset(vars_dict, attrs=attrs_dict)

    def guess_can_open(self, filename_or_obj: str | Path) -> bool:  # type: ignore[override]
        """Guess if this backend can open the file"""
        try:
            filename = Path(filename_or_obj)
            ext = filename.suffix.lower()
            return ext in [
                ".dbd",
                ".ebd",
                ".sbd",
                ".tbd",
                ".mbd",
                ".nbd",
                ".dcd",
                ".ecd",
                ".scd",
                ".tcd",
                ".mcd",
                ".ncd",
            ]
        except (TypeError, AttributeError):
            return False


def open_dbd_dataset(
    filename: str | Path,
    skip_first_record: bool = True,
    repair: bool = False,
    to_keep: list[str] | None = None,
    criteria: list[str] | None = None,
    drop_variables: list[str] | None = None,
    cache_dir: str | Path | None = None,
) -> xr.Dataset:
    """Open a single DBD file as an xarray Dataset.

    Parameters
    ----------
    filename : str or Path
        Path to the DBD file.
    skip_first_record : bool
        Skip the first data record (default True).
    repair : bool
        Attempt to repair corrupted records (default False).
    to_keep : list of str, optional
        Sensor names to keep. If None, all sensors are loaded.
    criteria : list of str, optional
        Sensor names for record selection criteria.
    drop_variables : list of str, optional
        Variable names to exclude from the returned Dataset.
    cache_dir : str, Path, or None
        Directory for sensor cache files. Defaults to ``<file_dir>/cache``.

    Returns
    -------
    xr.Dataset

    Examples
    --------
    >>> ds = open_dbd_dataset("test.sbd")
    >>> ds = open_dbd_dataset("test.sbd", to_keep=["m_depth", "m_lat"])
    """
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
    to_keep: list[str] | None = None,
    criteria: list[str] | None = None,
    skip_missions: list[str] | None = None,
    keep_missions: list[str] | None = None,
    cache_dir: str | Path | None = None,
) -> xr.Dataset:
    """Open multiple DBD files as a single concatenated xarray Dataset.

    Uses the C++ backend's two-pass approach with SensorsMap to merge sensor
    definitions across files, matching dbd2netCDF behavior exactly.

    Parameters
    ----------
    filenames : iterable of str or Path
        Paths to DBD files. Files are sorted internally.
    skip_first_record : bool
        Skip first record in each file except the first (default True).
    repair : bool
        Attempt to repair corrupted records (default False).
    to_keep : list of str, optional
        Sensor names to keep. If None, all sensors are loaded.
    criteria : list of str, optional
        Sensor names for record selection criteria.
    skip_missions : list of str, optional
        Mission names to exclude.
    keep_missions : list of str, optional
        Mission names to include (excludes all others).
    cache_dir : str, Path, or None
        Directory for sensor cache files.

    Returns
    -------
    xr.Dataset

    Examples
    --------
    >>> files = sorted(Path(".").glob("*.sbd"))
    >>> ds = open_multi_dbd_dataset(files)
    >>> ds = open_multi_dbd_dataset(files, to_keep=["m_depth", "m_present_time"])
    """
    if skip_missions and keep_missions:
        raise ValueError("Cannot specify both skip_missions and keep_missions")

    file_list = [str(Path(f)) for f in filenames]

    if not file_list:
        return xr.Dataset()

    cache_str = str(cache_dir) if cache_dir else ""

    try:
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
    except RuntimeError as e:
        raise OSError(f"Failed to read {len(file_list)} DBD files: {e}") from e

    columns = list(result["columns"])
    sensor_names = list(result["sensor_names"])
    sensor_units = list(result["sensor_units"])
    n_records = int(result["n_records"])
    n_files = int(result["n_files"])

    if to_keep:
        missing = set(to_keep) - set(sensor_names)
        if missing:
            logger.warning("Requested sensors not found in any file: %s", sorted(missing))

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

    return ds


# NetCDF dtype and fill value for each sensor byte-size
_NC_TYPE_INFO = {
    1: ("i1", np.int8(-127)),
    2: ("i2", np.int16(-32768)),
    4: ("f4", np.float32("nan")),
    8: ("f8", np.float64("nan")),
}


def write_multi_dbd_netcdf(
    filenames: Iterable[str | Path],
    output: str | Path,
    *,
    skip_first_record: bool = True,
    repair: bool = False,
    to_keep: list[str] | None = None,
    criteria: list[str] | None = None,
    skip_missions: list[str] | None = None,
    keep_missions: list[str] | None = None,
    cache_dir: str | Path | None = None,
    compression: int = 5,
) -> tuple[int, int]:
    """Stream multiple DBD files directly to a NetCDF file.

    Unlike :func:`open_multi_dbd_dataset` which loads all data into memory,
    this function reads one file at a time and writes its records to the
    output NetCDF immediately, keeping peak memory proportional to a single
    file's data.

    Parameters
    ----------
    filenames : iterable of str or Path
        Paths to DBD files.  Files are sorted internally.
    output : str or Path
        Path for the output NetCDF file.
    skip_first_record : bool
        Skip first record in each file except the first (default True).
    repair : bool
        Attempt to repair corrupted records (default False).
    to_keep : list of str, optional
        Sensor names to keep.  If None, all sensors are written.
    criteria : list of str, optional
        Sensor names for record selection criteria.
    skip_missions : list of str, optional
        Mission names to exclude.
    keep_missions : list of str, optional
        Mission names to include (excludes all others).
    cache_dir : str, Path, or None
        Directory for sensor cache files.
    compression : int
        Zlib compression level 0-9 (default 5, 0 disables compression).

    Returns
    -------
    tuple of (n_records, n_files)
    """
    import netCDF4

    if skip_missions and keep_missions:
        raise ValueError("Cannot specify both skip_missions and keep_missions")

    file_list = sorted(str(Path(f)) for f in filenames)
    if not file_list:
        return 0, 0

    cache_str = str(cache_dir) if cache_dir else ""

    # Pass 1: scan sensor union and valid files in one pass
    sensor_result = scan_sensors(
        file_list,
        cache_dir=cache_str,
        skip_missions=skip_missions or [],
        keep_missions=keep_missions or [],
    )
    sensor_names = list(sensor_result["sensor_names"])
    sensor_units = list(sensor_result["sensor_units"])
    sensor_sizes = list(sensor_result["sensor_sizes"])
    valid_files = list(sensor_result["valid_files"])

    if not valid_files or not sensor_names:
        return 0, 0

    # Apply to_keep filter to the union sensor list
    if to_keep:
        keep_set = set(to_keep)
        indices = [i for i, n in enumerate(sensor_names) if n in keep_set]
        sensor_names = [sensor_names[i] for i in indices]
        sensor_units = [sensor_units[i] for i in indices]
        sensor_sizes = [sensor_sizes[i] for i in indices]

    if not sensor_names:
        return 0, 0

    # Build fill value lookup for sensors missing from a batch
    fill_vals = {}
    for name, size in zip(sensor_names, sensor_sizes, strict=True):
        dtype, fill = _NC_TYPE_INFO.get(size, ("f8", np.float64("nan")))
        fill_vals[name] = (dtype, fill)

    # Create NetCDF file with variables
    chunk = 5000
    nc = netCDF4.Dataset(str(output), "w", format="NETCDF4")
    try:
        nc.createDimension("i", None)
        for name, units in zip(sensor_names, sensor_units, strict=True):
            dtype, _ = fill_vals[name]
            if compression > 0:
                v = nc.createVariable(  # type: ignore[call-overload]
                    name,
                    dtype,
                    ("i",),
                    fill_value=False,
                    zlib=True,
                    complevel=compression,
                    chunksizes=(chunk,),
                )
            else:
                v = nc.createVariable(name, dtype, ("i",), fill_value=False)
            v.units = units
    finally:
        nc.close()

    # Pass 2: read files in batches, append to NetCDF
    batch_size = 100
    offset = 0
    total_files = 0

    for batch_idx in range(0, len(valid_files), batch_size):
        batch_files = valid_files[batch_idx : batch_idx + batch_size]

        try:
            result = read_dbd_files(
                batch_files,
                cache_dir=cache_str,
                to_keep=to_keep or [],
                criteria=criteria or [],
                skip_missions=skip_missions or [],
                keep_missions=keep_missions or [],
                skip_first_record=skip_first_record,
                repair=repair,
            )
        except (OSError, RuntimeError, ValueError) as e:
            logger.warning("Error reading batch starting at index %d: %s", batch_idx, e)
            continue

        n = int(result["n_records"])
        batch_files_read = int(result["n_files"])

        # For batches after the first, the first file's first record overlaps
        # with the previous batch's last file — skip it
        start = 1 if (batch_idx > 0 and skip_first_record and n > 0) else 0
        n_write = n - start

        total_files += batch_files_read

        if n_write <= 0:
            continue

        # Build column map from this batch's result
        result_names = list(result["sensor_names"])
        result_cols = list(result["columns"])
        col_map = dict(zip(result_names, result_cols, strict=True))

        # Append to NetCDF
        nc = netCDF4.Dataset(str(output), "a")
        try:
            for name in sensor_names:
                col = col_map.get(name)
                if col is not None:
                    nc.variables[name][offset : offset + n_write] = col[start : start + n_write]
                else:
                    _, fill = fill_vals[name]
                    nc.variables[name][offset : offset + n_write] = np.full(n_write, fill)

            offset += n_write
            nc.setncattr("n_files", total_files)
            nc.setncattr("total_records", offset)
        finally:
            nc.close()

        # result goes out of scope — batch memory freed
        del result, result_cols, col_map

    return offset, total_files
