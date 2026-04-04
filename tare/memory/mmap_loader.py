"""
Memory-mapped I/O loader for large binary tick files.

Provides efficient streaming access to tick data without loading entire files into memory.
"""

import mmap
import struct
from pathlib import Path
from typing import Iterator, Optional


class MmapLoader:
    """Load large binary tick files using memory-mapped I/O."""

    # Binary format constants
    TICK_FORMAT_BINARY = 'binary'
    TICK_FORMAT_PACKED = 'packed'
    
    # Supported formats
    SUPPORTED_FORMATS = {TICK_FORMAT_BINARY, TICK_FORMAT_PACKED}
    
    # Struct format strings for unpacking
    # tick_id (8), timestamp (8), price (8), volume (8)
    PACKED_FORMAT = '>QQqQ'  # Big-endian: unsigned long long x3, signed long long x1
    PACKED_SIZE = struct.calcsize(PACKED_FORMAT)

    def __init__(self):
        """Initialize MmapLoader instance."""
        pass

    def preload_metadata(self, path: str) -> dict:
        """
        Extract metadata from binary tick file without full load.
        
        Args:
            path: Path to binary tick file
            
        Returns:
            Dictionary with keys:
            - 'file_size': Total file size in bytes (int)
            - 'tick_count': Number of ticks in file (int)
            - 'format': Detected format string
            
        Raises:
            FileNotFoundError: If file does not exist
            ValueError: If file format is invalid
        """
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        file_size = file_path.stat().st_size
        
        # Validate format based on file size
        if file_size % self.PACKED_SIZE == 0:
            tick_count = file_size // self.PACKED_SIZE
            detected_format = self.TICK_FORMAT_PACKED
        else:
            raise ValueError(f"Invalid file size {file_size}: not multiple of {self.PACKED_SIZE}")
        
        return {
            'file_size': file_size,
            'tick_count': tick_count,
            'format': detected_format,
        }

    def load(self, path: str, format: str) -> Iterator[dict]:
        """
        Stream ticks from binary file using memory mapping.
        
        Yields tick dictionaries with keys:
        - 'tick_id': Unique tick identifier (int)
        - 'timestamp': Unix timestamp in microseconds (int)
        - 'price': Price in base units (int, no decimals)
        - 'volume': Trade volume (int)
        
        Args:
            path: Path to binary tick file
            format: Format identifier ('packed' or 'binary')
            
        Yields:
            dict: Parsed tick data
            
        Raises:
            FileNotFoundError: If file does not exist
            ValueError: If format is unsupported
        """
        if format not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {format}. Supported: {self.SUPPORTED_FORMATS}")
        
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        with open(file_path, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mmapped_file:
                if format == self.TICK_FORMAT_PACKED:
                    yield from self._load_packed(mmapped_file)
                elif format == self.TICK_FORMAT_BINARY:
                    yield from self._load_binary(mmapped_file)

    def _load_packed(self, mmapped_file: mmap.mmap) -> Iterator[dict]:
        """
        Stream packed binary format ticks.
        
        Args:
            mmapped_file: Memory-mapped file object
            
        Yields:
            dict: Parsed tick data
        """
        offset = 0
        file_size = len(mmapped_file)
        
        while offset + self.PACKED_SIZE <= file_size:
            data = mmapped_file[offset:offset + self.PACKED_SIZE]
            
            tick_id, timestamp, price, volume = struct.unpack(
                self.PACKED_FORMAT, data
            )
            
            yield {
                'tick_id': tick_id,
                'timestamp': timestamp,
                'price': price,
                'volume': volume,
            }
            
            offset += self.PACKED_SIZE

    def _load_binary(self, mmapped_file: mmap.mmap) -> Iterator[dict]:
        """
        Stream raw binary format ticks.
        
        Args:
            mmapped_file: Memory-mapped file object
            
        Yields:
            dict: Parsed tick data
        """
        offset = 0
        file_size = len(mmapped_file)
        
        while offset + self.PACKED_SIZE <= file_size:
            data = mmapped_file[offset:offset + self.PACKED_SIZE]
            
            tick_id, timestamp, price, volume = struct.unpack(
                self.PACKED_FORMAT, data
            )
            
            yield {
                'tick_id': tick_id,
                'timestamp': timestamp,
                'price': price,
                'volume': volume,
            }
            
            offset += self.PACKED_SIZE
