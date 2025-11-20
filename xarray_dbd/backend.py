"""
Xarray backend engine for DBD files
"""

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
from xarray.backends import BackendEntrypoint
from xarray.backends.common import (
    AbstractDataStore,
    BackendArray,
)
from xarray.core import indexing

from .reader import DBDReader, read_multiple_dbd_files


class DBDBackendArray(BackendArray):
    """Lazy array wrapper for DBD data"""

    def __init__(
        self,
        filename: str | Path,
        sensor_name: str,
        sensor_index: int,
        shape: tuple[int],
        dtype: np.dtype,
        skip_first_record: bool = True,
        repair: bool = False,
    ):
        self.filename = Path(filename)
        self.sensor_name = sensor_name
        self.sensor_index = sensor_index
        self.shape = shape
        self.dtype = dtype
        self.skip_first_record = skip_first_record
        self.repair = repair
        self._data: np.ndarray | None = None

    def _load_data(self):
        """Load data from file (cached)"""
        if self._data is None:
            reader = DBDReader(
                self.filename,
                skip_first_record=self.skip_first_record,
                repair=self.repair,
            )
            data, _ = reader.read_data()
            self._data = data

        return self._data

    def __getitem__(self, key: indexing.ExplicitIndexer) -> np.typing.ArrayLike:
        return indexing.explicit_indexing_adapter(
            key,
            self.shape,
            indexing.IndexingSupport.BASIC,
            self._raw_indexing_method,
        )

    def _raw_indexing_method(self, key: tuple) -> np.typing.ArrayLike:
        data = self._load_data()
        # Extract the specific sensor column
        sensor_data = data[:, self.sensor_index]
        # Apply indexing
        return sensor_data[key[0]]


class DBDDataStore(AbstractDataStore):
    """Data store for DBD files"""

    def __init__(
        self,
        filename: str | Path,
        skip_first_record: bool = True,
        repair: bool = False,
        to_keep: list | None = None,
        criteria: list | None = None,
    ):
        self.filename = Path(filename)
        self.skip_first_record = skip_first_record
        self.repair = repair

        # Read file to get structure
        self.reader = DBDReader(
            self.filename,
            skip_first_record=skip_first_record,
            repair=repair,
            to_keep=to_keep,
            criteria=criteria,
        )

        # Get data to determine size
        self._data, self._metadata = self.reader.read_data()
        self._sensor_metadata = self.reader.get_sensor_metadata()

    def get_variables(self) -> dict[str, xr.Variable]:
        """Get xarray variables for all sensors"""
        variables: dict[str, xr.Variable] = {}

        output_sensors = self.reader.sensors.get_output_sensors()
        self._data.shape[0]

        # Variables use only 'i' dimension
        # (the 'j' dimension is declared in get_dimensions but not used)
        dims = ("i",)

        # Create variable for each sensor
        for sensor in output_sensors:
            if sensor.output_index is None:
                continue

            # Get data for this sensor
            data = self._data[:, sensor.output_index]

            # Create attributes
            attrs = {
                "units": sensor.units,
                "sensor_size": sensor.size,
            }

            # Create variable
            variables[sensor.name] = xr.Variable(
                dims,
                data,
                attrs=attrs,
            )

        return variables

    def get_attrs(self) -> dict[str, Any]:
        """Get global attributes"""
        return {
            "mission_name": self._metadata.get("mission_name", ""),
            "fileopen_time": self._metadata.get("fileopen_time", ""),
            "encoding_version": self._metadata.get("encoding_version", ""),
            "full_filename": self._metadata.get("full_filename", ""),
            "sensor_list_crc": self._metadata.get("sensor_list_crc", ""),
            "source_file": str(self.filename),
        }

    def get_dimensions(self) -> dict[str, int]:
        """Get dimensions"""
        return {"i": self._data.shape[0], "j": 1}


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
    ) -> xr.Dataset:
        """Open a DBD file as an xarray Dataset

        Parameters
        ----------
        filename_or_obj : str or Path
            Path to DBD file
        drop_variables : tuple of str, optional
            Variables to drop from the dataset
        skip_first_record : bool, default True
            Skip the first data record in the file
        repair : bool, default False
            Attempt to repair corrupted data records
        to_keep : list of str, optional
            List of sensor names to keep (default: all)
        criteria : list of str, optional
            List of sensor names to use as selection criteria

        Returns
        -------
        dataset : xarray.Dataset
        """
        filename = Path(filename_or_obj)

        store = DBDDataStore(
            filename,
            skip_first_record=skip_first_record,
            repair=repair,
            to_keep=to_keep,
            criteria=criteria,
        )

        vars_dict = store.get_variables()
        store.get_dimensions()
        attrs_dict = store.get_attrs()

        # Filter dropped variables
        if drop_variables:
            vars_dict = {k: v for k, v in vars_dict.items() if k not in drop_variables}

        # Create dataset
        dataset = xr.Dataset(vars_dict, attrs=attrs_dict)

        return dataset

    def guess_can_open(self, filename_or_obj: str | Path) -> bool:  # type: ignore[override]
        """Guess if this backend can open the file

        Checks for .dbd, .ebd, .sbd, .tbd, .mbd, .nbd extensions
        """
        try:
            filename = Path(filename_or_obj)
            ext = filename.suffix.lower()
            # Common DBD file extensions
            return ext in [".dbd", ".ebd", ".sbd", ".tbd", ".mbd", ".nbd"]
        except (TypeError, AttributeError):
            return False


def open_dbd_dataset(
    filename: str | Path,
    skip_first_record: bool = True,
    repair: bool = False,
    to_keep: list | None = None,
    criteria: list | None = None,
    drop_variables: list | None = None,
) -> xr.Dataset:
    """Open a DBD file as an xarray Dataset

    This is a convenience function that uses the DBD backend.

    Parameters
    ----------
    filename : str or Path
        Path to DBD file
    skip_first_record : bool, default True
        Skip the first data record in the file
    repair : bool, default False
        Attempt to repair corrupted data records
    to_keep : list of str, optional
        List of sensor names to keep (default: all)
    criteria : list of str, optional
        List of sensor names to use as selection criteria
    drop_variables : list of str, optional
        Variables to drop from the dataset

    Returns
    -------
    dataset : xarray.Dataset

    Examples
    --------
    >>> import xarray_dbd as xdbd
    >>> ds = xdbd.open_dbd_dataset('test.sbd')
    >>> print(ds)
    >>> print(ds['m_present_time'])
    """
    return xr.open_dataset(
        filename,
        engine=DBDBackendEntrypoint,
        skip_first_record=skip_first_record,
        repair=repair,
        to_keep=to_keep,
        criteria=criteria,
        drop_variables=drop_variables,
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

    This function efficiently reads multiple DBD files and concatenates
    them into a single dataset.

    Parameters
    ----------
    filenames : iterable of str or Path
        Paths to DBD files
    skip_first_record : bool, default True
        Skip the first data record in each file (except the first)
    repair : bool, default False
        Attempt to repair corrupted data records
    to_keep : list of str, optional
        List of sensor names to keep (default: all)
    criteria : list of str, optional
        List of sensor names to use as selection criteria
    skip_missions : list of str, optional
        Mission names to skip
    keep_missions : list of str, optional
        Mission names to keep (only these will be processed)
    cache_dir : str or Path, optional
        Directory containing sensor cache files (.cac)

    Returns
    -------
    dataset : xarray.Dataset

    Examples
    --------
    >>> import xarray_dbd as xdbd
    >>> from pathlib import Path
    >>> files = sorted(Path('.').glob('*.sbd'))
    >>> ds = xdbd.open_multi_dbd_dataset(files)
    >>> print(ds)
    """
    filenames = [Path(f) for f in filenames]

    # Read all files
    data, sensor_names, metadata = read_multiple_dbd_files(
        filenames,
        skip_first_record=skip_first_record,
        repair=repair,
        to_keep=to_keep,
        criteria=criteria,
        skip_missions=skip_missions,
        keep_missions=keep_missions,
        cache_dir=cache_dir,
    )

    # Get sensor metadata from first file
    first_reader = DBDReader(
        filenames[0],
        skip_first_record=skip_first_record,
        repair=repair,
        to_keep=to_keep,
        cache_dir=cache_dir,
        criteria=criteria,
    )
    sensor_metadata = first_reader.get_sensor_metadata()

    # Create dataset
    dims = ("i",)
    data_vars = {}

    for idx, sensor_name in enumerate(sensor_names):
        attrs = {
            "units": sensor_metadata.get(sensor_name, {}).get("units", ""),
        }
        data_vars[sensor_name] = xr.Variable(
            dims,
            data[:, idx],
            attrs=attrs,
        )

    attrs = {
        "n_files": metadata["n_files"],
        "total_records": metadata["total_records"],
    }

    # Create dataset
    ds = xr.Dataset(data_vars, attrs=attrs)

    # To match dbd2netCDF, we need a 'j' dimension with size 1
    # We create a dummy variable to establish the dimension, then drop it
    ds = ds.assign_coords(j=xr.DataArray([0], dims=["j"]))
    ds = ds.drop_vars("j")

    return ds
