#!/usr/bin/env python3
"""Debug why we're stopping after 1 record"""

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

    # Skip to known bytes
    while True:
        byte = fp.read(1)
        if not byte:
            break
        if byte == b's':
            peek = fp.read(15)
            if peek[0] == ord('a'):
                kb = KnownBytes(fp, data=byte + peek)
                print(f"Known bytes: flip={kb.flip_bytes}")
                break

    # Now read first few records
    n_sensors = len(sensors)
    header_bytes = (n_sensors + 3) // 4

    for record_num in range(5):
        # Read tag
        tag_byte = fp.read(1)
        if not tag_byte:
            print(f"  Record {record_num}: EOF")
            break

        tag = tag_byte[0]
        pos = fp.tell() - 1

        print(f"\n  Record {record_num} at position {pos}:")
        print(f"    Tag: 0x{tag:02x} = '{chr(tag) if 32 <= tag < 127 else '?'}'")

        if tag == ord('d'):
            print("    ✓ Valid 'd' tag")
            # Read header
            header_bits = fp.read(header_bytes)
            if len(header_bits) != header_bytes:
                print(f"    ✗ Short header: got {len(header_bits)}, expected {header_bytes}")
                break

            # Count codes
            codes = {0: 0, 1: 0, 2: 0, 3: 0}
            bytes_needed = 0
            for i in range(n_sensors):
                byte_idx = i >> 2
                bit_offset = 6 - ((i & 0x3) << 1)
                code = (header_bits[byte_idx] >> bit_offset) & 0x03
                codes[code] += 1
                if code == 2:
                    bytes_needed += sensors.sensors[i].size

            print(f"    Codes: {codes}")
            print(f"    Bytes to read for new values: {bytes_needed}")

            # Skip those bytes
            value_bytes = fp.read(bytes_needed)
            print(f"    Read {len(value_bytes)} value bytes")

            if len(value_bytes) != bytes_needed:
                print("    ✗ Short read")
                break

        elif tag == ord('X'):
            print("    End tag")
            break
        else:
            print("    ✗ Unexpected tag")
            # Check next few bytes
            next_bytes = fp.read(20)
            print(f"    Next 20 bytes: {next_bytes.hex()}")

            # Look for next 'd'
            for i, b in enumerate(next_bytes):
                if b == ord('d'):
                    print(f"    Found 'd' at offset +{i+1}")
                    break
            break
