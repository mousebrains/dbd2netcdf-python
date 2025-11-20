#!/usr/bin/env python3
"""Trace exact positions during parsing"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from xarray_dbd.decompression import open_dbd_file
from xarray_dbd.header import DBDHeader

test_file = Path("dbd_files/01330000.dcd")

# First, manually find where known bytes should be
with open_dbd_file(test_file, 'rb') as fp:
    all_data = fp.read()

# Search for 'sa' followed by 0x1234
for i in range(len(all_data) - 16):
    if (all_data[i] == ord('s') and
        all_data[i+1] == ord('a') and
        all_data[i+2] == 0x34 and
        all_data[i+3] == 0x12):
        print(f"Found known bytes signature at position {i}")
        print(f"  Bytes: {all_data[i:i+16].hex()}")
        break

# Now trace our parsing
print("\nOur parsing:")
with open_dbd_file(test_file, 'rb') as fp:
    header = DBDHeader(fp, str(test_file))
    pos1 = fp.tell()
    print(f"  After header: position ~{pos1}")

    # Read what's next
    next_100 = fp.read(100)
    print(f"  Next 100 bytes: {next_100.hex()[:80]}...")
    print(f"  As ASCII: {bytes([b if 32 <= b < 127 else ord('.') for b in next_100[:40]])}")
