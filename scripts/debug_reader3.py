#!/usr/bin/env python3
"""Debug data record parsing"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from xarray_dbd.header import DBDHeader
from xarray_dbd.reader import KnownBytes
from xarray_dbd.sensor import DBDSensor, DBDSensors

test_file = Path("dbd2netcdf/test/test.sbd")

with open(test_file, 'rb') as fp:
    # Read header
    header = DBDHeader(fp, str(test_file))

    # Read sensors
    sensors = DBDSensors()
    for _i in range(header.num_sensors):
        line = fp.readline().decode('ascii').strip()
        sensor = DBDSensor(line)
        sensors.add(sensor)

    # Read known bytes
    kb = KnownBytes(fp)

    n_sensors = len(sensors)
    header_bytes = (n_sensors + 3) // 4

    print(f"Number of sensors: {n_sensors}")
    print(f"Header bytes per record: {header_bytes}")
    print(f"Data starts at position: {fp.tell()}")

    # Read first record
    record_num = 0
    max_records = 3

    while record_num < max_records:
        start_pos = fp.tell()

        # Read tag
        tag_byte = fp.read(1)
        if not tag_byte:
            print(f"\nEOF at position {start_pos}")
            break

        tag = tag_byte[0]
        print(f"\n--- Record {record_num} at position {start_pos} ---")
        print(f"Tag: 0x{tag:02x} ('{chr(tag) if 32 <= tag < 127 else '?'}')")

        if tag == ord('X'):
            print("End tag found")
            break

        if tag != ord('d'):
            print(f"ERROR: Expected 'd' tag, got 0x{tag:02x}")
            break

        # Read header bits
        header_bits = fp.read(header_bytes)
        if len(header_bits) != header_bytes:
            print(f"ERROR: Expected {header_bytes} header bytes, got {len(header_bits)}")
            break

        print(f"Header bits: {header_bits[:20].hex()}... ({len(header_bits)} bytes)")

        # Decode header bits
        has_criteria = False
        sensors_with_data = []
        sensors_with_new_data = []

        for sensor_idx in range(min(20, n_sensors)):  # Check first 20 sensors
            sensor = sensors.sensors[sensor_idx]

            # Get 2-bit code
            byte_idx = sensor_idx >> 2
            bit_offset = 6 - ((sensor_idx & 0x3) << 1)
            code = (header_bits[byte_idx] >> bit_offset) & 0x03

            if code == 1:  # Repeat
                sensors_with_data.append((sensor.name, "repeat"))
                if sensor.criteria:
                    has_criteria = True
            elif code == 2:  # New value
                sensors_with_data.append((sensor.name, "new"))
                sensors_with_new_data.append(sensor)
                if sensor.criteria:
                    has_criteria = True

        print(f"Sensors with data (first 20): {len([x for x in sensors_with_data if x])} total")
        if sensors_with_data:
            print(f"  First few: {sensors_with_data[:5]}")
        print(f"Sensors with new data: {len(sensors_with_new_data)}")
        if sensors_with_new_data:
            print(f"  First few: {[s.name for s in sensors_with_new_data[:5]]}")
        print(f"Has criteria: {has_criteria}")

        # Try to read the new values
        if sensors_with_new_data:
            print("\nAttempting to read values...")
            # First, scan through ALL sensors to find where new values are
            bytes_to_read = 0
            for sensor_idx in range(n_sensors):
                byte_idx = sensor_idx >> 2
                bit_offset = 6 - ((sensor_idx & 0x3) << 1)
                code = (header_bits[byte_idx] >> bit_offset) & 0x03
                if code == 2:  # New value
                    sensor = sensors.sensors[sensor_idx]
                    bytes_to_read += sensor.size

            print(f"  Total bytes to read for new values: {bytes_to_read}")

            # Read those bytes
            value_bytes = fp.read(bytes_to_read)
            print(f"  Read {len(value_bytes)} bytes")
            if len(value_bytes) < bytes_to_read:
                print(f"  ERROR: Expected {bytes_to_read} bytes, got {len(value_bytes)}")
                break

        record_num += 1

    print(f"\nFinal position: {fp.tell()}")
    print(f"File size: {test_file.stat().st_size}")
