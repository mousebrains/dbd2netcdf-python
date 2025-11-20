"""
Efficient DBD file reader
"""

import struct
import numpy as np
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Tuple, Union
import warnings

from .header import DBDHeader
from .sensor import DBDSensor, DBDSensors
from .decompression import open_dbd_file


class KnownBytes:
    """Handles endianness detection from known bytes section"""

    def __init__(self, fp: BinaryIO, data: Optional[bytes] = None):
        """Read and detect endianness

        The known bytes section consists of:
        - 's' tag byte
        - 'a' byte
        - 0x1234 int16 (for endianness detection)
        - 123.456 float32
        - 123456789.12345 float64
        Total: 16 bytes

        Args:
            fp: File positioned at known bytes section
            data: Optional pre-read known bytes data (16 bytes)
        """
        # If data provided, use it; otherwise read from file
        if data is not None:
            if len(data) != 16:
                raise ValueError(f"Known bytes data must be 16 bytes, got {len(data)}")
            all_data = data
        else:
            all_data = fp.read(16)
            if len(all_data) != 16:
                raise ValueError(f"Could not read 16 bytes for known bytes, got {len(all_data)}")

        # Read tag
        tag = all_data[0:1]
        if not tag or tag[0] != ord('s'):
            raise ValueError(f"Invalid known bytes tag: expected 's', got {tag}")

        # Read 'a' byte
        byte_a = all_data[1:2]
        if not byte_a or byte_a[0] != ord('a'):
            raise ValueError(f"Invalid known bytes: expected 'a', got {byte_a}")

        # Read int16 for endianness detection
        int16_bytes = all_data[2:4]

        # Try little endian first
        int16_le = struct.unpack('<h', int16_bytes)[0]
        int16_be = struct.unpack('>h', int16_bytes)[0]

        if int16_le == 0x1234:
            self.flip_bytes = False
        elif int16_be == 0x1234:
            self.flip_bytes = True
        else:
            raise ValueError(f"Invalid known bytes int16: {int16_le:#x}")

        # Read and validate float32
        float32_bytes = all_data[4:8]

        fmt = '>f' if self.flip_bytes else '<f'
        float32_val = struct.unpack(fmt, float32_bytes)[0]
        if abs(float32_val - 123.456) > 0.001:
            raise ValueError(f"Invalid known bytes float: {float32_val}")

        # Read and validate float64
        float64_bytes = all_data[8:16]

        fmt = '>d' if self.flip_bytes else '<d'
        float64_val = struct.unpack(fmt, float64_bytes)[0]
        if abs(float64_val - 123456789.12345) > 0.001:
            raise ValueError(f"Invalid known bytes double: {float64_val}")

    @property
    def length(self) -> int:
        return 16


class DBDReader:
    """Efficient reader for DBD files - reads all data into memory for speed"""

    def __init__(self, filename: Union[str, Path],
                 skip_first_record: bool = True,
                 repair: bool = False,
                 to_keep: Optional[List[str]] = None,
                 criteria: Optional[List[str]] = None,
                 cache_dir: Optional[Union[str, Path]] = None):
        """Initialize DBD reader and load all data

        Args:
            filename: Path to DBD file
            skip_first_record: Skip first data record (default True)
            repair: Attempt to repair corrupted records
            to_keep: List of sensor names to keep (None = all)
            criteria: List of sensor names to use as selection criteria
            cache_dir: Directory containing sensor cache files (.cac)
        """
        self.filename = Path(filename)
        self.skip_first_record = skip_first_record
        self.repair = repair

        # Determine cache directory
        if cache_dir is None:
            # Look for cache subdirectory next to file
            cache_dir = self.filename.parent / "cache"
        else:
            cache_dir = Path(cache_dir)
        self.cache_dir = cache_dir

        # Read entire file in one pass
        with open_dbd_file(self.filename, 'rb') as fp:
            # Read header
            self.header = DBDHeader(fp, str(self.filename))
            if self.header.is_empty():
                raise ValueError(f"Empty or invalid header in {self.filename}")

            # Read sensor list (may use cache)
            self.sensors, known_bytes_data = self._read_sensors(fp)

            # Filter sensors
            self.sensors.filter_sensors(to_keep, criteria)

            # Read known bytes for endianness
            self.known_bytes = KnownBytes(fp, data=known_bytes_data)

            # Read all data immediately (speed over memory)
            self.data, self.metadata = self._read_all_data(fp)

    def _read_sensors(self, fp: BinaryIO) -> Tuple[DBDSensors, Optional[bytes]]:
        """Read sensor list from file or cache

        Returns:
            Tuple of (sensors, known_bytes_data)
            known_bytes_data is None if not pre-read
        """
        sensors = DBDSensors()
        num_sensors = self.header.num_sensors
        known_bytes_data = None

        # Check if sensor list is factored (cached)
        if self.header.is_factored and self.cache_dir:
            # Try to load from cache
            cache_file = self.cache_dir / f"{self.header.sensor_list_crc.lower()}.cac"

            if cache_file.exists():
                # Read from cache (only available sensors!)
                with open(cache_file, 'r') as cf:
                    for line in cf:
                        line = line.strip()
                        if line.startswith('s:'):
                            sensor = DBDSensor(line)
                            # Only add available sensors (marked with 'T')
                            if sensor.available:
                                sensors.add(sensor)

                # Skip over sensor list in file (it's binary/compressed)
                # Read bytes until we find the known bytes marker 's'
                while True:
                    byte = fp.read(1)
                    if not byte:
                        break
                    if byte == b's':
                        # This might be start of known bytes
                        # Read the next 15 bytes to make 16 total
                        peek = fp.read(15)
                        if len(peek) >= 1 and peek[0] == ord('a'):
                            # Looks like known bytes!
                            known_bytes_data = byte + peek
                            break
                        # Not known bytes, continue searching

                # Note: we only load available sensors, so count may be less than total
                if len(sensors) == 0:
                    warnings.warn(
                        f"No available sensors found in cache {cache_file}"
                    )

                return sensors, known_bytes_data
            else:
                warnings.warn(
                    f"Sensor list is factored but cache file {cache_file} not found"
                )
                # Fall through to read from file

        # Read sensor list from file
        for _ in range(num_sensors):
            line = fp.readline()
            if not line:
                break

            try:
                line = line.decode('ascii').strip()
            except UnicodeDecodeError:
                break

            if not line.startswith('s:'):
                break

            sensor = DBDSensor(line)
            sensors.add(sensor)

        if len(sensors) != num_sensors:
            warnings.warn(
                f"Expected {num_sensors} sensors, found {len(sensors)} in {self.filename}"
            )

        return sensors, known_bytes_data

    def _read_all_data(self, fp: BinaryIO) -> Tuple[np.ndarray, Dict[str, any]]:
        """Read all data from current file position

        Args:
            fp: File positioned at start of data section

        Returns:
            Tuple of (data_array, metadata_dict)
            data_array: 2D array of shape (n_records, n_sensors)
            metadata_dict: Dictionary of file metadata
        """
        # Pre-allocate array
        file_size = self.filename.stat().st_size
        estimated_records = max(100, file_size // 100)  # Rough estimate

        output_sensors = self.sensors.get_output_sensors()
        n_output = len(output_sensors)

        # Use NaN for missing values
        data = np.full((estimated_records, n_output), np.nan, dtype=np.float64)
        prev_values = np.full(n_output, np.nan, dtype=np.float64)

        n_records = 0
        n_sensors = len(self.sensors)
        header_bytes = (n_sensors + 3) // 4

        # Read records
        record_idx = 0
        while True:
            # Read tag
            tag_byte = fp.read(1)
            if not tag_byte:
                break

            tag = tag_byte[0]

            # Check for end tag
            if tag == ord('X'):
                break

            # Check for data tag
            if tag != ord('d'):
                # Unexpected tag - search for next 'd'
                # Allow limited search at start (padding), unlimited in repair mode
                search_limit = None if self.repair else (1000 if n_records == 0 else 0)

                found = False
                search_count = 0
                while search_limit is None or search_count < search_limit:
                    byte = fp.read(1)
                    if not byte:
                        break
                    search_count += 1
                    if byte[0] == ord('d'):
                        tag = ord('d')
                        found = True
                        break

                if not found:
                    # No more data records
                    break

            # Read header bits
            header_bits = fp.read(header_bytes)
            if len(header_bits) != header_bytes:
                break

            # Expand if needed
            if n_records >= len(data):
                new_size = len(data) + estimated_records
                data = np.resize(data, (new_size, n_output))
                data[n_records:] = np.nan

            # Process sensors
            has_criteria = False

            for sensor_idx in range(n_sensors):
                sensor = self.sensors.sensors[sensor_idx]

                # Get 2-bit code for this sensor
                byte_idx = sensor_idx >> 2
                bit_offset = 6 - ((sensor_idx & 0x3) << 1)
                code = (header_bits[byte_idx] >> bit_offset) & 0x03

                if code == 1:  # Repeat previous
                    if sensor.criteria:
                        has_criteria = True
                    if sensor.keep and sensor.output_index is not None:
                        data[n_records, sensor.output_index] = prev_values[sensor.output_index]

                elif code == 2:  # New value
                    # IMPORTANT: Must read value from file even if not keeping this sensor!
                    try:
                        value = sensor.read_value(fp, self.known_bytes.flip_bytes)
                    except (EOFError, struct.error):
                        # End of file or corrupt data
                        break

                    # Now decide whether to keep it
                    if sensor.criteria:
                        has_criteria = True
                    if sensor.keep and sensor.output_index is not None:
                        data[n_records, sensor.output_index] = value
                        prev_values[sensor.output_index] = value

            # Only keep record if it has criteria data
            if has_criteria:
                # Skip first record if requested
                if not self.skip_first_record or record_idx > 0:
                    n_records += 1
                record_idx += 1

        # Trim to actual size
        data = data[:n_records]

        # Build metadata
        metadata = {
            'filename': str(self.filename),
            'mission_name': self.header.mission_name,
            'fileopen_time': self.header.fileopen_time,
            'encoding_version': self.header.encoding_version,
            'full_filename': self.header.full_filename,
            'sensor_list_crc': self.header.sensor_list_crc,
            'n_records': n_records,
        }

        return data, metadata

    def read_data(self) -> Tuple[np.ndarray, Dict[str, any]]:
        """Get the already-loaded data

        Returns:
            Tuple of (data_array, metadata_dict)
        """
        return self.data, self.metadata

    def get_sensor_metadata(self) -> Dict[str, Dict[str, any]]:
        """Get metadata for all output sensors"""
        metadata = {}
        for sensor in self.sensors.get_output_sensors():
            metadata[sensor.name] = {
                'units': sensor.units,
                'size': sensor.size,
                'dtype': str(sensor.dtype),
            }
        return metadata


def read_multiple_dbd_files(
    filenames: List[Union[str, Path]],
    skip_first_record: bool = True,
    repair: bool = False,
    to_keep: Optional[List[str]] = None,
    criteria: Optional[List[str]] = None,
    skip_missions: Optional[List[str]] = None,
    keep_missions: Optional[List[str]] = None,
    cache_dir: Optional[Union[str, Path]] = None,
) -> Tuple[np.ndarray, List[str], Dict[str, Dict]]:
    """Read multiple DBD files and concatenate

    Args:
        filenames: List of DBD files to read
        skip_first_record: Skip first record in each file
        repair: Attempt to repair corrupted files
        to_keep: List of sensor names to keep
        criteria: List of sensor names for selection
        skip_missions: List of mission names to skip
        keep_missions: List of mission names to keep

    Returns:
        Tuple of (data, sensor_names, metadata)
    """
    skip_set = set(m.lower() for m in skip_missions) if skip_missions else None
    keep_set = set(m.lower() for m in keep_missions) if keep_missions else None

    all_data = []
    sensor_names = None
    file_metadata = []

    for filename in filenames:
        try:
            reader = DBDReader(
                filename,
                skip_first_record=skip_first_record,
                repair=repair,
                to_keep=to_keep,
                criteria=criteria,
                cache_dir=cache_dir
            )

            # Check mission filter
            if not reader.header.should_process_mission(skip_set, keep_set):
                continue

            # Read data
            data, metadata = reader.read_data()

            if len(data) == 0:
                continue

            # Get sensor names from first file
            if sensor_names is None:
                sensor_names = [s.name for s in reader.sensors.get_output_sensors()]

            all_data.append(data)
            file_metadata.append(metadata)

        except Exception as e:
            warnings.warn(f"Error reading {filename}: {e}")
            continue

    if not all_data:
        raise ValueError("No valid data found in any files")

    # Concatenate all data
    combined_data = np.concatenate(all_data, axis=0)

    # Combine metadata
    combined_metadata = {
        'files': file_metadata,
        'total_records': len(combined_data),
        'n_files': len(file_metadata),
    }

    return combined_data, sensor_names, combined_metadata
