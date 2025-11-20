#!/usr/bin/env python3
"""Debug LZ4 decompression - are we reading all frames?"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import lz4.block
import struct

test_file = Path("dbd_files/01330000.dcd")

print(f"File size: {test_file.stat().st_size} bytes")

# Manually decompress and count frames
with open(test_file, 'rb') as fp:
    # Read magic bytes
    magic = fp.read(8)
    print(f"Magic bytes: {magic.hex()}")

    total_decompressed = 0
    frame_count = 0
    buffer_size = 65536

    while True:
        # Try to read frame size
        size_bytes = fp.read(2)
        if len(size_bytes) < 2:
            print(f"\nEnd of file at frame {frame_count}")
            break

        frame_size = struct.unpack('>H', size_bytes)[0]

        # Read compressed data
        compressed_data = fp.read(frame_size)
        if len(compressed_data) < frame_size:
            print(f"\nIncomplete frame {frame_count}: got {len(compressed_data)}, expected {frame_size}")
            break

        # Decompress
        try:
            decompressed = lz4.block.decompress(compressed_data, uncompressed_size=buffer_size)
            frame_count += 1
            total_decompressed += len(decompressed)

            if frame_count <= 5 or frame_count % 10 == 0:
                print(f"Frame {frame_count}: compressed={frame_size}, decompressed={len(decompressed)}, total={total_decompressed}")
        except Exception as e:
            print(f"\nError decompressing frame {frame_count}: {e}")
            break

    print(f"\nTotal frames: {frame_count}")
    print(f"Total decompressed: {total_decompressed} bytes")

# Now check what our reader gives us
print("\n" + "="*60)
print("Checking our LZ4DecompressingReader:")

from xarray_dbd.decompression import open_dbd_file

with open_dbd_file(test_file, 'rb') as fp:
    # Read everything
    all_data = fp.read()
    print(f"Total bytes read by our reader: {len(all_data)}")

    # Try to read more
    more = fp.read(1000)
    print(f"Tried to read 1000 more bytes, got: {len(more)}")
