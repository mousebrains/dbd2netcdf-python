#!/usr/bin/env python3
"""Test reader with detailed logging"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))



from xarray_dbd.decompression import open_dbd_file
from xarray_dbd.header import DBDHeader
from xarray_dbd.reader import KnownBytes
from xarray_dbd.sensor import DBDSensor, DBDSensors

test_file = Path("dbd_files/01330000.dcd")
cache_dir = Path("dbd_files/cache")

with open_dbd_file(test_file, 'rb') as fp:
    # Read header
    header = DBDHeader(fp, str(test_file))
    print(f"Header: {header.mission_name}, {header.num_sensors} sensors")

    # Read sensors from cache
    cache_file = cache_dir / f"{header.sensor_list_crc.lower()}.cac"
    sensors = DBDSensors()
    with open(cache_file) as cf:
        for line in cf:
            if line.strip().startswith('s:'):
                sensors.add(DBDSensor(line.strip()))

    sensors.filter_sensors(to_keep=None, criteria=None)

    # Skip to known bytes
    while True:
        byte = fp.read(1)
        if not byte:
            break
        if byte == b's':
            peek = fp.read(15)
            if peek[0] == ord('a'):
                kb = KnownBytes(fp, data=byte + peek)
                break

    # Read all records
    n_sensors = len(sensors)
    header_bytes = (n_sensors + 3) // 4

    record_count = 0
    while True:
        # Read tag
        tag_byte = fp.read(1)
        if not tag_byte:
            print(f"\nStopped at record {record_count}: EOF")
            break

        tag = tag_byte[0]

        # Search for 'd' if needed
        if tag != ord('d'):
            found = False
            search_count = 0
            while True:
                byte = fp.read(1)
                if not byte:
                    print(f"\nStopped at record {record_count}: EOF during search")
                    break
                search_count += 1
                if byte[0] == ord('d'):
                    tag = ord('d')
                    found = True
                    break

            if not found:
                print(f"\nStopped at record {record_count}: No more 'd' tags found after searching {search_count} bytes")
                break

        # Check for end tag
        if tag == ord('X'):
            print(f"\nStopped at record {record_count}: Found 'X' end tag")
            break

        # Read header bits
        header_bits = fp.read(header_bytes)
        if len(header_bits) != header_bytes:
            print(f"\nStopped at record {record_count}: Short header read ({len(header_bits)} / {header_bytes})")
            break

        # Count bytes to read
        bytes_to_read = 0
        for sensor_idx in range(n_sensors):
            sensor = sensors.sensors[sensor_idx]
            byte_idx = sensor_idx >> 2
            bit_offset = 6 - ((sensor_idx & 0x3) << 1)
            code = (header_bits[byte_idx] >> bit_offset) & 0x03

            if code == 2:  # New value
                bytes_to_read += sensor.size

        # Read values
        value_bytes = fp.read(bytes_to_read)
        if len(value_bytes) != bytes_to_read:
            print(f"\nStopped at record {record_count}: Short value read ({len(value_bytes)} / {bytes_to_read})")
            break

        record_count += 1
        if record_count % 100 == 0:
            print(f"Read {record_count} records...", end='\r')

    print(f"\n\nTotal records read: {record_count}")
