#!/usr/bin/env python3
"""Debug script to check sensor availability and criteria"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from xarray_dbd.header import DBDHeader
from xarray_dbd.sensor import DBDSensor, DBDSensors

test_file = Path("dbd2netcdf/test/test.sbd")

with open(test_file, 'rb') as fp:
    # Read header
    header = DBDHeader(fp, str(test_file))

    # Read sensors
    sensors = DBDSensors()
    for _i in range(header.num_sensors):
        line = fp.readline().decode('ascii').strip()
        sensor = DBDSensor(line)
        sensors.add(sensor)

    print(f"Total sensors: {len(sensors)}")

    # Check availability
    available = [s for s in sensors if s.available]
    print(f"Available sensors: {len(available)}")
    print(f"  First few available: {[s.name for s in available[:5]]}")

    # Check keep status
    keep = [s for s in sensors if s.keep]
    print(f"Sensors to keep: {len(keep)}")

    # Check criteria status
    criteria = [s for s in sensors if s.criteria]
    print(f"Criteria sensors: {len(criteria)}")
    print(f"  First few criteria: {[s.name for s in criteria[:5]]}")

    # All sensors start with keep=True and criteria=True by default
    print("\nNote: By default, all sensors have keep=True and criteria=True")
    print("The issue is that if NO sensor has new data in a record,")
    print("the record won't be kept (has_criteria will be False)")
