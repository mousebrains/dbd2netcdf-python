#!/usr/bin/env python3
"""Debug to find missing bytes"""

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
    for i in range(header.num_sensors):
        line = fp.readline().decode('ascii').strip()
        sensor = DBDSensor(line)
        sensors.add(sensor)

    # Read known bytes
    kb = KnownBytes(fp)

    n_sensors = len(sensors)
    header_bytes = (n_sensors + 3) // 4

    # Read first record tag
    tag = fp.read(1)[0]
    assert tag == ord('d')
    pos_after_tag = fp.tell()

    # Read header bits
    header_bits = fp.read(header_bytes)
    pos_after_header = fp.tell()

    print(f"Position after 'd' tag: {pos_after_tag}")
    print(f"Position after header bits: {pos_after_header}")

    # Count codes
    code_counts = {0: 0, 1: 0, 2: 0, 3: 0}
    bytes_to_read = 0
    sensors_with_code2 = []

    for sensor_idx in range(n_sensors):
        byte_idx = sensor_idx >> 2
        bit_offset = 6 - ((sensor_idx & 0x3) << 1)
        code = (header_bits[byte_idx] >> bit_offset) & 0x03
        code_counts[code] += 1

        if code == 2:
            sensor = sensors.sensors[sensor_idx]
            sensors_with_code2.append((sensor_idx, sensor.name, sensor.size))
            bytes_to_read += sensor.size

    print("\nCode distribution:")
    for code, count in code_counts.items():
        print(f"  Code {code}: {count} sensors")

    print(f"\nTotal bytes to read for code==2: {bytes_to_read}")
    print(f"Number of sensors with code==2: {len(sensors_with_code2)}")

    # Now actually read them
    bytes_read = 0
    for _idx, _name, size in sensors_with_code2:
        value_bytes = fp.read(size)
        bytes_read += len(value_bytes)

    pos_after_values = fp.tell()
    print(f"\nPosition after reading {bytes_read} bytes: {pos_after_values}")
    print(f"Expected position: {pos_after_header + bytes_to_read}")

    # What's next?
    next_10 = fp.read(10)
    print("\nNext 10 bytes:")
    print(f"  Hex: {next_10.hex()}")
    print(f"  First byte: 0x{next_10[0]:02x}")

    if next_10[0] == ord('d'):
        print("  ✓ Found next 'd' tag!")
    else:
        print(f"  ✗ Expected 'd' (0x64), got 0x{next_10[0]:02x}")

        # How many bytes until we find the next 'd'?
        fp.seek(pos_after_values)
        search_bytes = fp.read(100)
        for i, b in enumerate(search_bytes):
            if b == ord('d'):
                print(f"  Next 'd' tag found {i} bytes ahead at position {pos_after_values + i}")
                print(f"  Missing bytes (hex): {search_bytes[:i].hex()}")
                break
