"""
DBD file header parsing
"""

import contextlib
from typing import BinaryIO


class DBDHeader:
    """Parses and stores DBD file header information"""

    def __init__(self, fp: BinaryIO, filename: str):
        """Read header from file

        Args:
            fp: File object positioned at start of file
            filename: Filename for error reporting
        """
        self.filename = filename
        self.records: dict[str, str] = {}
        self._parse(fp)

    def _parse(self, fp: BinaryIO):
        """Parse header lines"""
        num_lines = 10  # Default number of header lines

        for _ in range(1000):  # Safety limit
            line_bytes = fp.readline()
            if not line_bytes:
                break

            try:
                line = line_bytes.decode("ascii").strip()
            except UnicodeDecodeError:
                # Hit binary data, stop parsing
                break

            if ":" not in line:
                # Hit sensor list or binary data
                break

            key, _, value = line.partition(":")
            key = key.strip()
            value = value.strip()

            self.records[key] = value

            # Update expected number of lines
            if key == "num_ascii_tags":
                with contextlib.suppress(ValueError):
                    num_lines = int(value)

            if len(self.records) >= num_lines:
                break

    def get(self, key: str, default: str = "") -> str:
        """Get a header value"""
        return self.records.get(key, default)

    def get_int(self, key: str, default: int = 0) -> int:
        """Get a header value as integer"""
        value = self.get(key)
        if not value:
            return default
        try:
            return int(value)
        except ValueError:
            return default

    @property
    def mission_name(self) -> str:
        """Get mission name"""
        return self.get("mission_name")

    @property
    def num_sensors(self) -> int:
        """Get total number of sensors"""
        return self.get_int("total_num_sensors")

    @property
    def sensor_list_crc(self) -> str:
        """Get sensor list CRC"""
        return self.get("sensor_list_crc")

    @property
    def is_factored(self) -> bool:
        """Check if sensor list is factored"""
        return self.get_int("sensor_list_factored") != 0

    @property
    def fileopen_time(self) -> str:
        """Get file open time"""
        return self.get("fileopen_time")

    @property
    def encoding_version(self) -> str:
        """Get encoding version"""
        return self.get("encoding_ver")

    @property
    def full_filename(self) -> str:
        """Get full filename"""
        return self.get("full_filename")

    @property
    def the8x3_filename(self) -> str:
        """Get 8x3 filename"""
        return self.get("the8x3_filename")

    @property
    def filename_extension(self) -> str:
        """Get filename extension"""
        return self.get("filename_extension")

    def is_empty(self) -> bool:
        """Check if header is empty"""
        return len(self.records) == 0

    def should_process_mission(
        self, skip_missions: set[str] | None = None, keep_missions: set[str] | None = None
    ) -> bool:
        """Check if this file's mission should be processed

        Args:
            skip_missions: Set of mission names to skip
            keep_missions: Set of mission names to keep

        Returns:
            True if mission should be processed
        """
        if skip_missions is None:
            skip_missions = set()
        if keep_missions is None:
            keep_missions = set()

        if not skip_missions and not keep_missions:
            return True

        mission = self.mission_name.lower()

        if skip_missions and mission in skip_missions:
            return False

        if keep_missions:
            return mission in keep_missions

        return True

    def __repr__(self):
        return f"DBDHeader(mission={self.mission_name}, sensors={self.num_sensors})"
