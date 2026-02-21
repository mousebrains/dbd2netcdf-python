#!/usr/bin/env python3
"""Debug compressed file reading"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from xarray_dbd.decompression import open_dbd_file
from xarray_dbd.header import DBDHeader
from xarray_dbd.sensor import DBDSensor, DBDSensors

test_file = Path("dbd_files/01330000.dcd")

print("Opening compressed file...")
with open_dbd_file(test_file, 'rb') as fp:
    # Read header
    print("\n1. Reading header...")
    header = DBDHeader(fp, str(test_file))
    print(f"   Mission: {header.mission_name}")
    print(f"   Expected sensors: {header.num_sensors}")
    print(f"   Position after header: {fp.tell()}")

    # Read sensors
    print(f"\n2. Reading {header.num_sensors} sensors...")
    sensors = DBDSensors()
    for i in range(min(10, header.num_sensors)):  # Read first 10
        line = fp.readline()
        if not line:
            print(f"   EOF at sensor {i}")
            break

        try:
            line_str = line.decode('ascii').strip()
            if not line_str.startswith('s:'):
                print(f"   Non-sensor line at {i}: {line_str[:50]}")
                break
            sensor = DBDSensor(line_str)
            sensors.add(sensor)
            if i < 5:
                print(f"   {i}: {sensor.name}")
        except Exception as e:
            print(f"   Error at sensor {i}: {e}")
            break

    print(f"   Read {len(sensors)} sensors")
    print(f"   Position: {fp.tell()}")

    # Try reading next 20 bytes
    print("\n3. Next 20 bytes:")
    next_bytes = fp.read(20)
    print(f"   Hex: {next_bytes.hex()}")
    print(f"   ASCII: {repr(next_bytes)}")
