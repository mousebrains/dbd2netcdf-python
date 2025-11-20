#!/usr/bin/env python3
"""Debug individual value reading"""

import sys
import struct
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
    print(f"Endianness: {'big' if kb.flip_bytes else 'little'} endian")

    n_sensors = len(sensors)
    header_bytes = (n_sensors + 3) // 4

    # Read first record tag
    tag = fp.read(1)[0]
    assert tag == ord('d')

    # Read header bits
    header_bits = fp.read(header_bytes)

    # Find sensors with new data and read them one by one
    print("\nReading values one by one:")
    value_count = 0
    for sensor_idx in range(n_sensors):
        byte_idx = sensor_idx >> 2
        bit_offset = 6 - ((sensor_idx & 0x3) << 1)
        code = (header_bits[byte_idx] >> bit_offset) & 0x03

        if code == 2:  # New value
            sensor = sensors.sensors[sensor_idx]

            # Read value
            value_bytes = fp.read(sensor.size)
            if len(value_bytes) != sensor.size:
                print(f"ERROR: EOF reading sensor {sensor.name}")
                break

            # Decode based on size
            try:
                if sensor.size == 1:
                    value = struct.unpack('b', value_bytes)[0]
                elif sensor.size == 2:
                    fmt = '>h' if kb.flip_bytes else '<h'
                    value = struct.unpack(fmt, value_bytes)[0]
                elif sensor.size == 4:
                    fmt = '>f' if kb.flip_bytes else '<f'
                    value = struct.unpack(fmt, value_bytes)[0]
                elif sensor.size == 8:
                    fmt = '>d' if kb.flip_bytes else '<d'
                    value = struct.unpack(fmt, value_bytes)[0]
                else:
                    value = None

                value_count += 1
                if value_count <= 10:
                    print(f"  {value_count:3d}. {sensor.name:40s} = {value}")
            except struct.error as e:
                print(f"ERROR decoding {sensor.name}: {e}")
                break

    print(f"\nTotal values read: {value_count}")
    print(f"Current position: {fp.tell()}")

    # Check what's next
    next_byte = fp.read(1)
    if next_byte:
        print(f"Next byte: 0x{next_byte[0]:02x} = '{chr(next_byte[0]) if 32 <= next_byte[0] < 127 else '?'}'")
        if next_byte[0] == ord('d'):
            print("  -> Next record!")
        elif next_byte[0] == ord('X'):
            print("  -> End of data")
        else:
            print("  -> Unexpected!")
    else:
        print("EOF")
