"""
Sensor metadata and collections for DBD files
"""

import struct
from typing import BinaryIO

import numpy as np


class DBDSensor:
    """Represents a single sensor in a DBD file"""

    def __init__(self, line: str):
        """Parse a sensor line from DBD file

        Format: s: <used> <index> <storage_index> <size> <name> <units>
        Example: s: T 0 0 8 m_present_time timestamp
        """
        parts = line.split()
        if len(parts) < 7 or parts[0] != "s:":
            raise ValueError(f"Invalid sensor line: {line}")

        self.available = parts[1] == "T"
        self.file_index = int(parts[2])
        self.storage_index = int(parts[3])
        self.size = int(parts[4])
        self.name = parts[5]
        self.units = parts[6] if len(parts) > 6 else ""

        # Will be set later during sensor mapping
        self.keep = True
        self.criteria = True
        self.output_index: int | None = None

    @property
    def dtype(self) -> np.dtype:
        """Get numpy dtype for this sensor"""
        if self.size == 1:
            return np.dtype("int8")
        elif self.size == 2:
            return np.dtype("int16")
        elif self.size == 4:
            return np.dtype("float32")
        elif self.size == 8:
            return np.dtype("float64")
        else:
            raise ValueError(f"Unsupported sensor size: {self.size}")

    def read_value(self, fp: BinaryIO, flip_bytes: bool) -> float:
        """Read a single value from file stream"""
        data = fp.read(self.size)
        if len(data) != self.size:
            raise EOFError(f"Expected {self.size} bytes, got {len(data)}")

        if self.size == 1:
            return float(struct.unpack("b", data)[0])
        elif self.size == 2:
            fmt = ">h" if flip_bytes else "<h"
            return float(struct.unpack(fmt, data)[0])
        elif self.size == 4:
            fmt = ">f" if flip_bytes else "<f"
            return struct.unpack(fmt, data)[0]
        elif self.size == 8:
            fmt = ">d" if flip_bytes else "<d"
            return struct.unpack(fmt, data)[0]
        else:
            raise ValueError(f"Unsupported size: {self.size}")

    def __repr__(self):
        return f"DBDSensor(name={self.name}, size={self.size}, units={self.units})"


class DBDSensors:
    """Collection of sensors for a DBD file"""

    def __init__(self):
        self.sensors: list[DBDSensor] = []
        self._by_name: dict[str, DBDSensor] = {}
        self._by_file_index: dict[int, DBDSensor] = {}

    def add(self, sensor: DBDSensor):
        """Add a sensor to the collection"""
        self.sensors.append(sensor)
        self._by_name[sensor.name] = sensor
        self._by_file_index[sensor.file_index] = sensor

    def get_by_name(self, name: str) -> DBDSensor | None:
        """Get sensor by name"""
        return self._by_name.get(name)

    def get_by_index(self, index: int) -> DBDSensor | None:
        """Get sensor by file index"""
        return self._by_file_index.get(index)

    def __len__(self):
        return len(self.sensors)

    def __iter__(self):
        return iter(self.sensors)

    def __getitem__(self, index):
        if isinstance(index, str):
            return self._by_name[index]
        return self.sensors[index]

    def get_output_sensors(self) -> list[DBDSensor]:
        """Get list of sensors marked for output"""
        return [s for s in self.sensors if s.keep]

    def set_output_indices(self):
        """Assign output indices to sensors marked for keeping"""
        idx = 0
        for sensor in self.sensors:
            if sensor.keep:
                sensor.output_index = idx
                idx += 1

    def filter_sensors(self, to_keep: list[str] | None = None, criteria: list[str] | None = None):
        """Filter which sensors to keep and which are criteria

        Args:
            to_keep: List of sensor names to keep (None = keep all)
            criteria: List of sensor names to use as selection criteria
        """
        # If to_keep specified, mark all as not keep first
        if to_keep is not None:
            for sensor in self.sensors:
                sensor.keep = sensor.name in to_keep

        # Mark criteria sensors
        if criteria is not None:
            for sensor in self.sensors:
                sensor.criteria = sensor.name in criteria

        # Assign output indices
        self.set_output_indices()
