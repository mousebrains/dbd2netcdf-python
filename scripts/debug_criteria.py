#!/usr/bin/env python3
"""Debug criteria logic - why are we not keeping records?"""

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

    print(f"Loaded {len(sensors)} sensors from cache")

    # Filter sensors with criteria=None (should mark all as criteria)
    print("\n=== BEFORE filtering ===")
    print(f"Sensors: {len(sensors)}")

    sensors.filter_sensors(to_keep=None, criteria=None)

    print("\n=== AFTER filtering ===")
    print(f"Output sensors: {len(sensors.get_output_sensors())}")

    # Check how many have criteria set
    criteria_count = sum(1 for s in sensors.sensors if s.criteria)
    keep_count = sum(1 for s in sensors.sensors if s.keep)

    print(f"Sensors with criteria=True: {criteria_count}")
    print(f"Sensors with keep=True: {keep_count}")

    # Show first few sensors
    print("\nFirst 10 sensors:")
    for i, s in enumerate(sensors.sensors[:10]):
        print(f"  {i}: {s.name:30s} keep={s.keep} criteria={s.criteria}")

    # Skip to known bytes
    while True:
        byte = fp.read(1)
        if not byte:
            break
        if byte == b's':
            peek = fp.read(15)
            if peek[0] == ord('a'):
                kb = KnownBytes(fp, data=byte + peek)
                print(f"\nKnown bytes: flip={kb.flip_bytes}")
                break

    # Now read first few records and check has_criteria
    n_sensors = len(sensors)
    header_bytes = (n_sensors + 3) // 4

    print("\n=== Reading records ===")
    for record_num in range(10):
        # Read tag
        tag_byte = fp.read(1)
        if not tag_byte:
            print(f"Record {record_num}: EOF")
            break

        tag = tag_byte[0]

        # Search for 'd' tag
        if tag != ord('d'):
            found = False
            for search_idx in range(2000):  # Larger search window
                byte = fp.read(1)
                if not byte:
                    break
                if byte[0] == ord('d'):
                    tag = ord('d')
                    found = True
                    print(f"Record {record_num}: Found 'd' after skipping {search_idx + 1} bytes")
                    break
                elif byte[0] == ord('X'):
                    print(f"Record {record_num}: Found 'X' (end tag)")
                    break
            if not found:
                print(f"Record {record_num}: No 'd' tag found")
                break

        if tag != ord('d'):
            break

        # Read header bits
        header_bits = fp.read(header_bytes)
        if len(header_bits) != header_bytes:
            print(f"Record {record_num}: Short header")
            break

        # Process sensors - check for criteria
        has_criteria = False
        bytes_to_read = 0

        for sensor_idx in range(n_sensors):
            sensor = sensors.sensors[sensor_idx]

            # Get 2-bit code
            byte_idx = sensor_idx >> 2
            bit_offset = 6 - ((sensor_idx & 0x3) << 1)
            code = (header_bits[byte_idx] >> bit_offset) & 0x03

            if code == 1:  # Repeat
                if sensor.criteria:
                    has_criteria = True
            elif code == 2:  # New value
                bytes_to_read += sensor.size
                if sensor.criteria:
                    has_criteria = True

        print(f"Record {record_num}: has_criteria={has_criteria}, bytes_to_read={bytes_to_read}")

        # Skip the value bytes
        value_bytes = fp.read(bytes_to_read)
        if len(value_bytes) != bytes_to_read:
            print(f"  Short read: got {len(value_bytes)}, expected {bytes_to_read}")
            break
