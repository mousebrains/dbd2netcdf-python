"""Error codes and exception class compatible with dbdreader."""

from __future__ import annotations

from collections import namedtuple

DBD_ERROR_CACHE_NOT_FOUND = 1
DBD_ERROR_NO_VALID_PARAMETERS = 2
DBD_ERROR_NO_TIME_VARIABLE = 3
DBD_ERROR_NO_FILE_CRITERIUM_SPECIFIED = 4
DBD_ERROR_NO_FILES_FOUND = 5
DBD_ERROR_NO_DATA_TO_INTERPOLATE_TO = 6
DBD_ERROR_CACHEDIR_NOT_FOUND = 7
DBD_ERROR_ALL_FILES_BANNED = 8
DBD_ERROR_INVALID_DBD_FILE = 9
DBD_ERROR_INVALID_ENCODING = 10
DBD_ERROR_INVALID_FILE_CRITERION_SPECIFIED = 11
DBD_ERROR_NO_DATA_TO_INTERPOLATE = 12
DBD_ERROR_NO_DATA = 13
DBD_ERROR_READ_ERROR = 14


class DbdError(Exception):
    MissingCacheFileData = namedtuple("MissingCacheFileData", "missing_cache_files cache_dir")

    def __init__(self, value=9, mesg=None, data=None):
        self.value = value
        self.mesg = mesg
        self.data = data

    def __str__(self):
        if self.value == DBD_ERROR_NO_VALID_PARAMETERS:
            mesg = "The requested parameter(s) was(were) not found."
        elif self.value == DBD_ERROR_NO_TIME_VARIABLE:
            mesg = "The time variable was not found."
        elif self.value == DBD_ERROR_CACHE_NOT_FOUND:
            mesg = "Cache file was not found."
        elif self.value == DBD_ERROR_NO_FILE_CRITERIUM_SPECIFIED:
            mesg = "No file specification supplied (list of filenames or pattern)"
        elif self.value == DBD_ERROR_NO_FILES_FOUND:
            mesg = "No files were found."
        elif self.value == DBD_ERROR_NO_DATA_TO_INTERPOLATE_TO:
            mesg = "No data to interpolate to."
        elif self.value == DBD_ERROR_CACHEDIR_NOT_FOUND:
            mesg = "Cache file directory does not exist."
        elif self.value == DBD_ERROR_ALL_FILES_BANNED:
            mesg = "All data files were banned."
        elif self.value == DBD_ERROR_INVALID_DBD_FILE:
            mesg = "Invalid DBD file."
        elif self.value == DBD_ERROR_INVALID_ENCODING:
            mesg = "Invalid encoding version encountered."
        elif self.value == DBD_ERROR_INVALID_FILE_CRITERION_SPECIFIED:
            mesg = "Invalid or conflicting file selection criterion/criteria specified."
        elif self.value == DBD_ERROR_NO_DATA_TO_INTERPOLATE:
            mesg = "One or more parameters that are to be interpolated, does/do not have any data."
        elif self.value == DBD_ERROR_NO_DATA:
            mesg = "One or more parameters do not have any data."
        elif self.value == DBD_ERROR_READ_ERROR:
            mesg = "Error reading DBD file."
        else:
            mesg = f"Undefined error. ({self.value})"
        if self.mesg:
            mesg = " ".join((mesg, self.mesg))
        return mesg
