"""Type stubs for the _dbd_cpp C++ extension module."""

from typing import Any

def read_dbd_file(
    filename: str,
    cache_dir: str = "",
    to_keep: list[str] = ...,
    criteria: list[str] = ...,
    skip_first_record: bool = True,
    repair: bool = False,
) -> dict[str, Any]: ...
def read_dbd_files(
    filenames: list[str],
    cache_dir: str = "",
    to_keep: list[str] = ...,
    criteria: list[str] = ...,
    skip_missions: list[str] = ...,
    keep_missions: list[str] = ...,
    skip_first_record: bool = True,
    repair: bool = False,
) -> dict[str, Any]: ...
def scan_sensors(
    filenames: list[str],
    cache_dir: str = "",
    skip_missions: list[str] = ...,
    keep_missions: list[str] = ...,
) -> dict[str, Any]: ...
def scan_headers(
    filenames: list[str],
    skip_missions: list[str] = ...,
    keep_missions: list[str] = ...,
) -> dict[str, Any]: ...
