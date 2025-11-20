#!/usr/bin/env python3
"""Read entire decompressed file and search for all 'd' and 'X' tags"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from xarray_dbd.decompression import open_dbd_file
from xarray_dbd.header import DBDHeader

test_file = Path("dbd_files/01330000.dcd")

with open_dbd_file(test_file, 'rb') as fp:
    # Read header
    header = DBDHeader(fp, str(test_file))
    header_pos = fp.tell()
    print(f"Header ends at position: {header_pos}")
    print(f"Expected sensors: {header.num_sensors}")

    # Read rest of file
    all_data = fp.read()
    print(f"Total decompressed data after header: {len(all_data)} bytes")

    # Search for 'd' tags
    d_positions = []
    X_positions = []

    for i, byte in enumerate(all_data):
        if byte == ord('d'):
            d_positions.append(i)
        elif byte == ord('X'):
            X_positions.append(i)

    print(f"\nFound {len(d_positions)} 'd' tags")
    print(f"Found {len(X_positions)} 'X' tags")

    if d_positions:
        print("\nFirst 20 'd' tag positions:")
        for i, pos in enumerate(d_positions[:20]):
            print(f"  {i}: position {pos} (absolute: {header_pos + pos})")

    if X_positions:
        print("\nFirst 20 'X' tag positions:")
        for i, pos in enumerate(X_positions[:20]):
            # Check what's around this X
            start = max(0, pos - 5)
            end = min(len(all_data), pos + 6)
            context = all_data[start:end].hex()
            print(f"  {i}: position {pos} (absolute: {header_pos + pos}), context: {context}")

    # Check specifically around position where we think we see 'X'
    # From debug_criteria.py, record 1 ended and we found 'X'
    # Let's see what the data looks like there
    print("\n=== Checking data around our stopping point ===")
    # After record 0 (6701 bytes) + record 1 (found after 241 bytes + header + 590 bytes)
    # Let's search for it
    search_start = 7000  # Around where record 1 ends
    search_end = min(len(all_data), search_start + 1000)

    print(f"Searching bytes {search_start} to {search_end}")
    for i in range(search_start, search_end):
        if all_data[i] == ord('X'):
            context_start = max(0, i - 20)
            context_end = min(len(all_data), i + 20)
            print(f"  Found 'X' at position {i}")
            print(f"  Context: {all_data[context_start:context_end].hex()}")
            break
