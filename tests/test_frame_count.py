#!/usr/bin/env python3
"""Count LZ4 frames read by our decompressor"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import struct

import lz4.block

test_file = Path("dbd_files/01330000.dcd")

# Count frames manually
print("Counting frames in file:")
print("=" * 60)

with open(test_file, 'rb') as fp:
    frame_sizes = []
    while True:
        size_bytes = fp.read(2)
        if len(size_bytes) < 2:
            break

        frame_size = struct.unpack('>H', size_bytes)[0]
        frame_sizes.append(frame_size)

        # Skip the compressed data
        fp.seek(frame_size, 1)

print(f"Total frames in file: {len(frame_sizes)}")
print(f"Frame sizes: min={min(frame_sizes)}, max={max(frame_sizes)}, avg={sum(frame_sizes)/len(frame_sizes):.0f}")

# Now test decompression with explicit frame count
print("\n" + "=" * 60)
print("Decompressing frames:")

with open(test_file, 'rb') as fp:
    total_decompressed = 0
    frames_ok = 0

    for i in range(len(frame_sizes)):
        size_bytes = fp.read(2)
        if len(size_bytes) < 2:
            print(f"EOF at frame {i}")
            break

        frame_size = struct.unpack('>H', size_bytes)[0]
        compressed_data = fp.read(frame_size)

        if len(compressed_data) < frame_size:
            print(f"Short read at frame {i}")
            break

        try:
            # Try different buffer sizes
            for buffer_size in [65536, 131072, 262144]:
                try:
                    decompressed = lz4.block.decompress(compressed_data, uncompressed_size=buffer_size)
                    frames_ok += 1
                    total_decompressed += len(decompressed)
                    if i % 10 == 0:
                        print(f"Frame {i}: compressed={frame_size}, decompressed={len(decompressed)}")
                    break
                except:
                    if buffer_size == 262144:
                        raise
        except Exception as e:
            print(f"Error at frame {i}: {e}")
            print(f"  Frame size: {frame_size}")
            print(f"  First 20 bytes: {compressed_data[:20].hex()}")
            break

print(f"\nSuccessfully decompressed: {frames_ok} / {len(frame_sizes)} frames")
print(f"Total decompressed: {total_decompressed} bytes ({total_decompressed / 1024 / 1024:.2f} MB)")
