#!/usr/bin/env python3
"""Test with only available sensors"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import xarray_dbd as xdbd
from xarray_dbd.reader import DBDReader

test_file = Path("dbd2netcdf/test/test.sbd")

# First, get list of available sensors
reader = DBDReader(test_file, skip_first_record=False)
available_sensors = [s.name for s in reader.sensors if s.available]

print(f"Available sensors: {len(available_sensors)}")
print(f"  {available_sensors}")

# Now read with only those sensors
print(f"\nReading with only available sensors...")
ds = xdbd.open_dbd_dataset(
    test_file,
    to_keep=available_sensors,
    skip_first_record=False,  # Don't skip first record
    repair=False,
)

print(f"✓ Successfully opened file")
print(f"  Records: {len(ds.i)}")
print(f"  Variables: {list(ds.data_vars)}")

# Check if we got the expected 94 records
if len(ds.i) == 94:
    print(f"\n✓✓✓ SUCCESS! Got expected 94 records")
else:
    print(f"\n  Expected 94 records, got {len(ds.i)}")
