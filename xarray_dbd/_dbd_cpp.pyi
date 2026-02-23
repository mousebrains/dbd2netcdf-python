"""Type stubs for the _dbd_cpp C++ extension module."""

from typing import Any, TypedDict

class _SingleResult(TypedDict):
    columns: list[Any]
    sensor_names: list[str]
    sensor_units: list[str]
    sensor_sizes: list[int]
    n_records: int
    header: dict[str, str]
    filename: str

class _MultiResult(TypedDict):
    columns: list[Any]
    sensor_names: list[str]
    sensor_units: list[str]
    sensor_sizes: list[int]
    n_records: int
    n_files: int

class _ScanResult(TypedDict):
    sensor_names: list[str]
    sensor_units: list[str]
    sensor_sizes: list[int]
    valid_files: list[str]
    n_files: int

class _HeaderResult(TypedDict):
    valid_files: list[str]
    mission_names: list[str]
    sensor_list_crcs: list[str]

def read_dbd_file(
    filename: str,
    cache_dir: str = "",
    to_keep: list[str] = ...,
    criteria: list[str] = ...,
    skip_first_record: bool = True,
    repair: bool = False,
) -> _SingleResult: ...
def read_dbd_files(
    filenames: list[str],
    cache_dir: str = "",
    to_keep: list[str] = ...,
    criteria: list[str] = ...,
    skip_missions: list[str] = ...,
    keep_missions: list[str] = ...,
    skip_first_record: bool = True,
    repair: bool = False,
) -> _MultiResult: ...
def scan_sensors(
    filenames: list[str],
    cache_dir: str = "",
    skip_missions: list[str] = ...,
    keep_missions: list[str] = ...,
) -> _ScanResult: ...
def scan_headers(
    filenames: list[str],
    skip_missions: list[str] = ...,
    keep_missions: list[str] = ...,
) -> _HeaderResult: ...
