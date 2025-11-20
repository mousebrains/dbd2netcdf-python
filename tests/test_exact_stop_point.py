#!/usr/bin/env python3
"""Find exact point where reading stops"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from xarray_dbd.decompression import open_dbd_file
from xarray_dbd.header import DBDHeader
from xarray_dbd.sensor import DBDSensors, DBDSensor
from xarray_dbd.reader import KnownBytes

test_file = Path("dbd_files/01330000.dcd")
cache_dir = Path("dbd_files/cache")

with open_dbd_file(test_file, 'rb') as fp:
    # Read header
    header = DBDHeader(fp, str(test_file))
    print(f"Header read complete")
    header_end_pos = fp.tell()
    print(f"Position after header: ~{header_end_pos}")

    # Read sensors from cache
    cache_file = cache_dir / f"{header.sensor_list_crc.lower()}.cac"
    sensors = DBDSensors()
    with open(cache_file, 'r') as cf:
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

    known_bytes_end = fp.tell()
    print(f"Position after known bytes: ~{known_bytes_end}")

    # Try to read to record 790
    n_sensors = len(sensors)
    header_bytes = (n_sensors + 3) // 4
    print(f"Header bytes per record: {header_bytes}")

    record_count = 0
    total_value_bytes = 0

    for target in [788, 789, 790]:
        while record_count < target:
            # Read tag
            tag_byte = fp.read(1)
            if not tag_byte:
                print(f"\nEOF before reaching record {target} (at record {record_count})")
                break

            tag = tag_byte[0]

            # Search for 'd' if needed
            if tag != ord('d'):
                found = False
                while True:
                    byte = fp.read(1)
                    if not byte:
                        break
                    if byte[0] == ord('d'):
                        tag = ord('d')
                        found = True
                        break
                if not found:
                    print(f"\nNo 'd' tag found before record {target} (at record {record_count})")
                    break

            if tag == ord('X'):
                print(f"\n'X' tag before record {target} (at record {record_count})")
                break

            # Read header bits
            header_bits = fp.read(header_bytes)
            if len(header_bits) != header_bytes:
                print(f"\nShort header at record {record_count}: got {len(header_bits)}, expected {header_bytes}")
                print(f"Approximate position: ~{known_bytes_end + total_value_bytes + record_count * (1 + header_bytes)}")

                # Check if we can read ANY more data
                more = fp.read(100)
                print(f"Can read {len(more)} more bytes from stream")
                if len(more) > 0:
                    print(f"  Next bytes: {more[:20].hex()}")
                break

            # Count bytes to read
            bytes_to_read = 0
            for sensor_idx in range(n_sensors):
                sensor = sensors.sensors[sensor_idx]
                byte_idx = sensor_idx >> 2
                bit_offset = 6 - ((sensor_idx & 0x3) << 1)
                code = (header_bits[byte_idx] >> bit_offset) & 0x03
                if code == 2:
                    bytes_to_read += sensor.size

            # Read values
            value_bytes = fp.read(bytes_to_read)
            if len(value_bytes) != bytes_to_read:
                print(f"\nShort value read at record {record_count}: got {len(value_bytes)}, expected {bytes_to_read}")
                break

            record_count += 1
            total_value_bytes += bytes_to_read

        if record_count >= target:
            print(f"✓ Successfully read record {record_count - 1}")
