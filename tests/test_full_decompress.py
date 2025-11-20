#!/usr/bin/env python3
"""Manually decompress entire file and compare"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import struct
import lz4.block

test_file = Path("dbd_files/01330000.dcd")

print("Manual decompression (no buffer size limit):")
print("=" * 60)

with open(test_file, 'rb') as fp:
    total_decompressed = 0
    frame_count = 0

    while True:
        # Try to read frame size
        size_bytes = fp.read(2)
        if len(size_bytes) < 2:
            print(f"EOF after {frame_count} frames")
            break

        # Big-endian uint16
        frame_size = struct.unpack('>H', size_bytes)[0]

        # Read compressed data
        compressed_data = fp.read(frame_size)
        if len(compressed_data) < frame_size:
            print(f"Incomplete frame {frame_count}")
            break

        # Decompress WITHOUT specifying uncompressed_size
        try:
            decompressed = lz4.block.decompress(compressed_data)
            frame_count += 1
            total_decompressed += len(decompressed)

            if frame_count <= 5:
                print(f"Frame {frame_count}: compressed={frame_size}, decompressed={len(decompressed)}")
        except Exception as e:
            print(f"Error at frame {frame_count}: {e}")
            break

print(f"\nTotal frames: {frame_count}")
print(f"Total decompressed: {total_decompressed} bytes ({total_decompressed / 1024 / 1024:.2f} MB)")

print("\n" + "=" * 60)
print("Our decompressor:")

from xarray_dbd.decompression import open_dbd_file

with open_dbd_file(test_file, 'rb') as fp:
    all_data = fp.read()
    print(f"Total bytes read: {len(all_data)} ({len(all_data) / 1024 / 1024:.2f} MB)")

print("\n" + "=" * 60)
if total_decompressed == len(all_data):
    print("✓ Decompression matches!")
else:
    print(f"✗ Mismatch: manual={total_decompressed}, ours={len(all_data)}")
    print(f"  Difference: {total_decompressed - len(all_data)} bytes")
