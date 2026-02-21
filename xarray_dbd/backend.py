"""
Xarray backend engine for DBD files using C++ parser
"""

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import xarray as xr
from xarray.backends import BackendEntrypoint

from ._dbd_cpp import read_dbd_file, read_dbd_files

logger = logging.getLogger(__name__)

__all__ = [
    "DBDDataStore",
    "DBDBackendEntrypoint",
    "open_dbd_dataset",
    "open_multi_dbd_dataset",
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
        drop_variables: tuple[str] | None = None,
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
            vars_dict = {k: v for k, v in vars_dict.items() if k not in drop_variables}

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

    # To match dbd2netCDF, we need a 'j' dimension with size 1
    ds = ds.assign_coords(j=xr.DataArray([0], dims=["j"]))
    ds = ds.drop_vars("j")

    return ds
