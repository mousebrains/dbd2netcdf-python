"""
LZ4 decompression support for compressed DBD files (*.?c? format)
"""

import struct
from pathlib import Path
from typing import BinaryIO, Union
import io

try:
    import lz4.block
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False


def is_compressed(filename: Union[str, Path]) -> bool:
    """Check if filename matches compressed DBD pattern *.?c?

    Examples: .dcd, .ecd, .scd, .tcd, .mcd, .ncd
    """
    suffix = Path(filename).suffix.lower()
    return len(suffix) == 4 and suffix[2] == 'c'


class LZ4DecompressingReader:
    """Reader that decompresses LZ4-compressed DBD files on the fly

    Format:
    - 2 bytes: big-endian uint16 frame size
    - N bytes: LZ4 compressed frame
    - Repeat until EOF

    The decompressed output is presented as a continuous stream.
    """

    def __init__(self, fp: BinaryIO, buffer_size: int = 65536):
        """Initialize decompressing reader

        Args:
            fp: File object to read compressed data from
            buffer_size: Maximum uncompressed frame size (default 64KB)
        """
        if not HAS_LZ4:
            raise ImportError(
                "lz4 package required for compressed DBD files. "
                "Install with: pip install lz4"
            )

        self.fp = fp
        self.buffer_size = buffer_size
        self.buffer = bytearray()
        self.buffer_pos = 0
        self.eof = False

    def _read_frame(self) -> bool:
        """Read and decompress next LZ4 frame

        Returns:
            True if frame was read, False if EOF
        """
        # Read frame size
        size_bytes = self.fp.read(2)
        if len(size_bytes) != 2:
            return False

        # Big-endian uint16
        frame_size = struct.unpack('>H', size_bytes)[0]

        # Read compressed frame
        compressed_data = self.fp.read(frame_size)
        if len(compressed_data) != frame_size:
            return False

        # Decompress
        try:
            decompressed = lz4.block.decompress(
                compressed_data,
                uncompressed_size=self.buffer_size
            )
        except Exception as e:
            raise IOError(f"LZ4 decompression failed: {e}")

        # Append to buffer
        self.buffer.extend(decompressed)

        return True

    def read(self, size: int = -1) -> bytes:
        """Read and decompress data

        Args:
            size: Number of bytes to read (-1 for all)

        Returns:
            Decompressed bytes
        """
        if size == -1:
            # Read everything
            result = bytearray(self.buffer[self.buffer_pos:])
            self.buffer_pos = len(self.buffer)

            # Read remaining frames
            while not self.eof:
                if not self._read_frame():
                    self.eof = True
                    break
                result.extend(self.buffer[self.buffer_pos:])
                self.buffer_pos = len(self.buffer)

            return bytes(result)

        # Read specific size
        result = bytearray()

        while len(result) < size and not self.eof:
            # Get from buffer
            available = len(self.buffer) - self.buffer_pos
            needed = size - len(result)

            if available > 0:
                take = min(available, needed)
                result.extend(self.buffer[self.buffer_pos:self.buffer_pos + take])
                self.buffer_pos += take

            # Need more data?
            if len(result) < size:
                # Clean up buffer
                if self.buffer_pos > 0:
                    self.buffer = self.buffer[self.buffer_pos:]
                    self.buffer_pos = 0

                # Read next frame
                if not self._read_frame():
                    self.eof = True
                    break

        return bytes(result)

    def readline(self) -> bytes:
        """Read a line (until \\n)

        Returns:
            Line including \\n, or partial line if EOF
        """
        result = bytearray()

        while not self.eof:
            # Search for newline in buffer
            start = self.buffer_pos
            try:
                newline_pos = self.buffer.index(b'\n'[0], start)
                # Found newline
                result.extend(self.buffer[start:newline_pos + 1])
                self.buffer_pos = newline_pos + 1
                break
            except ValueError:
                # No newline in buffer
                result.extend(self.buffer[start:])
                self.buffer_pos = len(self.buffer)

                # Clean up buffer
                self.buffer = bytearray()
                self.buffer_pos = 0

                # Read next frame
                if not self._read_frame():
                    self.eof = True
                    break

        return bytes(result)

    def tell(self) -> int:
        """Return approximate position in decompressed stream

        Note: This is approximate because we don't track exactly
        """
        # This is a simplified implementation
        # For exact tracking, would need to count all decompressed bytes
        return self.buffer_pos

    def close(self):
        """Close the underlying file"""
        if hasattr(self.fp, 'close'):
            self.fp.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
        return False


def open_dbd_file(filename: Union[str, Path], mode: str = 'rb') -> BinaryIO:
    """Open a DBD file, handling decompression if needed

    Args:
        filename: Path to DBD file (.dbd, .dcd, .ebd, .ecd, etc.)
        mode: File mode (should be 'rb' for binary read)

    Returns:
        File-like object (decompressing if compressed)

    Raises:
        ImportError: If file is compressed but lz4 not installed
    """
    fp = open(filename, mode)

    if is_compressed(filename):
        return LZ4DecompressingReader(fp)
    else:
        return fp
