#!/usr/bin/env python3
"""Debug sensor caching"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from xarray_dbd.decompression import open_dbd_file
from xarray_dbd.header import DBDHeader

test_file = Path("dbd_files/01330000.dcd")
cache_dir = Path("dbd_files/cache")

print("Opening compressed file...")
with open_dbd_file(test_file, 'rb') as fp:
    header = DBDHeader(fp, str(test_file))
    print(f"Mission: {header.mission_name}")
    print(f"Encoding version: {header.encoding_version}")
    print(f"Sensor list CRC: {header.sensor_list_crc}")
    print(f"Factored: {header.is_factored}")
    print(f"Expected sensors: {header.num_sensors}")

# Look for cache file
crc = header.sensor_list_crc.lower()
cache_file = cache_dir / f"{crc}.cac"

print(f"\nLooking for cache file: {cache_file}")
if cache_file.exists():
    print(f"✓ Found cache file ({cache_file.stat().st_size} bytes)")

    # Read sensors from cache
    with open(cache_file, 'r') as fp:
        lines = fp.readlines()

    print(f"  Cache has {len(lines)} lines")
    print(f"  First 5 lines:")
    for line in lines[:5]:
        print(f"    {line.strip()}")
else:
    print(f"✗ Cache file not found")
    # List what's in cache
    cache_files = sorted(cache_dir.glob("*.cac"))
    print(f"\n  Available cache files ({len(cache_files)}):")
    for cf in cache_files[:10]:
        print(f"    {cf.name}")
