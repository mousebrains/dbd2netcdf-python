#!/usr/bin/env python3
"""Debug script to understand DBD file structure"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from xarray_dbd.header import DBDHeader
from xarray_dbd.reader import KnownBytes
from xarray_dbd.sensor import DBDSensor, DBDSensors

test_file = Path("dbd2netcdf/test/test.sbd")

with open(test_file, 'rb') as fp:
    # Read header
    print("Reading header...")
    header = DBDHeader(fp, str(test_file))
    print(f"  Mission: {header.mission_name}")
    print(f"  Sensors: {header.num_sensors}")
    print(f"  Position after header: {fp.tell()}")

    # Read sensors
    print(f"\nReading {header.num_sensors} sensors...")
    sensors = DBDSensors()
    for i in range(header.num_sensors):
        line = fp.readline()
        if not line:
            print(f"  EOF at sensor {i}")
            break
        try:
            line_str = line.decode('ascii').strip()
            if not line_str.startswith('s:'):
                print(f"  Non-sensor line at {i}: {line_str[:50]}")
                fp.seek(fp.tell() - len(line))
                break
            sensor = DBDSensor(line_str)
            sensors.add(sensor)
        except UnicodeDecodeError:
            print(f"  Binary data at sensor {i}, position {fp.tell()}")
            fp.seek(fp.tell() - len(line))
            break

    print(f"  Read {len(sensors)} sensors")
    print(f"  Position after sensors: {fp.tell()}")

    # Read known bytes
    print("\nReading known bytes...")
    try:
        kb = KnownBytes(fp)
        print(f"  Flip bytes: {kb.flip_bytes}")
        print(f"  Position after known bytes: {fp.tell()}")
    except Exception as e:
        print(f"  ERROR: {e}")
        sys.exit(1)

    # Check what's next
    print("\nChecking data section...")
    pos = fp.tell()
    next_bytes = fp.read(20)
    print(f"  Next 20 bytes (hex): {next_bytes.hex()}")
    print(f"  Next 20 bytes (repr): {repr(next_bytes)}")
    print(f"  First byte: 0x{next_bytes[0]:02x} ('{chr(next_bytes[0]) if 32 <= next_bytes[0] < 127 else '?'}')")

    # Try to read first record
    fp.seek(pos)
    tag = fp.read(1)[0]
    print(f"\n  First tag: 0x{tag:02x} ('{chr(tag) if 32 <= tag < 127 else '?'}')")

    if tag == ord('d'):
        print("  ✓ Found 'd' data tag")
        header_bytes_needed = (header.num_sensors + 3) // 4
        print(f"  Header bytes needed: {header_bytes_needed}")
        header_bits = fp.read(header_bytes_needed)
        print(f"  Read {len(header_bits)} header bytes")
        print(f"  Header bits (hex): {header_bits[:10].hex()}...")
    elif tag == ord('X'):
        print("  Found 'X' end tag - no data in file")
    else:
        print(f"  ✗ Unexpected tag: 0x{tag:02x}")
