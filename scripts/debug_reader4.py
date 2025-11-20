#!/usr/bin/env python3
"""Debug byte counting for new values"""

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

    # Read header bits
    header_bits = fp.read(header_bytes)

    # Analyze which sensors have new data
    new_value_sensors = []
    for sensor_idx in range(n_sensors):
        byte_idx = sensor_idx >> 2
        bit_offset = 6 - ((sensor_idx & 0x3) << 1)
        code = (header_bits[byte_idx] >> bit_offset) & 0x03

        if code == 2:  # New value
            sensor = sensors.sensors[sensor_idx]
            new_value_sensors.append(sensor)

    print(f"Total sensors with new values: {len(new_value_sensors)}")
    print("\nFirst 20 sensors with new data:")
    for i, sensor in enumerate(new_value_sensors[:20]):
        print(f"  {i:3d}. {sensor.name:40s} size={sensor.size}")

    total_bytes = sum(s.size for s in new_value_sensors)
    print(f"\nTotal bytes to read: {total_bytes}")

    # Check what comes after
    pos_before = fp.tell()
    data_bytes = fp.read(total_bytes)
    pos_after = fp.tell()

    print(f"\nPosition before reading values: {pos_before}")
    print(f"Position after reading values: {pos_after}")
    print(f"Bytes read: {len(data_bytes)}")

    # What's next?
    next_20 = fp.read(20)
    print("\nNext 20 bytes after data:")
    print(f"  Hex: {next_20.hex()}")
    print(f"  ASCII: {repr(next_20)}")
    print(f"  First byte: 0x{next_20[0]:02x} = '{chr(next_20[0]) if 32 <= next_20[0] < 127 else '?'}'")

    # Check if it's end of data
    if next_20[0] == ord('X'):
        print("  -> This is the 'X' end-of-data marker!")
    elif next_20[0] == ord('d'):
        print("  -> This is another 'd' data record tag")
    else:
        print("  -> Unexpected byte!")

    # How much data is left?
    current_pos = fp.tell() - 20
    fp.seek(0, 2)  # Seek to end
    file_size = fp.tell()
    remaining = file_size - current_pos
    print(f"\nFile size: {file_size}")
    print(f"Current position: {current_pos}")
    print(f"Remaining bytes: {remaining}")
