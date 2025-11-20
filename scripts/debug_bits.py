#!/usr/bin/env python3
"""Debug bit extraction"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from xarray_dbd.header import DBDHeader
from xarray_dbd.sensor import DBDSensor, DBDSensors
from xarray_dbd.reader import KnownBytes

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

    # Read first record
    tag = fp.read(1)[0]
    header_bits = fp.read(header_bytes)

    # Check first byte of header bits
    first_byte = header_bits[0]
    print(f"First header byte: 0x{first_byte:02x} = {first_byte:08b}")
    print("Bit layout (MSB to LSB):")
    print("  Bits 7-6 (sensor 0): {}".format((first_byte >> 6) & 0x03))
    print("  Bits 5-4 (sensor 1): {}".format((first_byte >> 4) & 0x03))
    print("  Bits 3-2 (sensor 2): {}".format((first_byte >> 2) & 0x03))
    print("  Bits 1-0 (sensor 3): {}".format((first_byte >> 0) & 0x03))

    # Show first 4 sensors
    print("\nFirst 4 sensors:")
    for i in range(4):
        sensor = sensors.sensors[i]
        byte_idx = i >> 2
        bit_offset = 6 - ((i & 0x3) << 1)
        code = (header_bits[byte_idx] >> bit_offset) & 0x03
        print(f"  {i}: {sensor.name:40s} code={code} size={sensor.size}")

    # Now show last few sensors
    print(f"\nLast 4 sensors (indices {n_sensors-4} to {n_sensors-1}):")
    for i in range(n_sensors-4, n_sensors):
        sensor = sensors.sensors[i]
        byte_idx = i >> 2
        bit_offset = 6 - ((i & 0x3) << 1)
        code = (header_bits[byte_idx] >> bit_offset) & 0x03
        print(f"  {i}: {sensor.name:40s} code={code} size={sensor.size}")

    # Check last byte
    last_byte = header_bits[-1]
    last_sensor_idx = n_sensors - 1
    last_byte_idx = last_sensor_idx >> 2

    print(f"\nLast header byte (index {last_byte_idx}): 0x{header_bits[last_byte_idx]:02x}")

    # Calculate how many sensors fit in the last byte
    sensors_in_last_byte = 4 - ((n_sensors - 1) % 4) - 1
    if sensors_in_last_byte < 0:
        sensors_in_last_byte = 0
    print(f"Number of 2-bit codes in last byte: {4 - sensors_in_last_byte} used, {sensors_in_last_byte} unused")
