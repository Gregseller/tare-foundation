"""
Tests for mmap_loader module.
"""

import struct
import tempfile
from pathlib import Path

import pytest

from tare.memory.mmap_loader import MmapLoader


@pytest.fixture
def loader():
    """Create MmapLoader instance."""
    return MmapLoader()


@pytest.fixture
def temp_packed_file():
    """Create temporary file with packed binary ticks."""
    ticks = [
        (1, 1000000, 50000, 100),
        (2, 1000010, 50010, 200),
        (3, 1000020, 49990, 150),
    ]
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as f:
        for tick_id, timestamp, price, volume in ticks:
            data = struct.pack('>QQqQ', tick_id, timestamp, price, volume)
            f.write(data)
        temp_path = f.name
    
    yield temp_path, ticks
    
    Path(temp_path).unlink()


def test_preload_metadata_packed(loader, temp_packed_file):
    """Test metadata extraction from packed binary file."""
    file_path, ticks = temp_packed_file
    
    metadata = loader.preload_metadata(file_path)
    
    assert metadata['tick_count'] == 3
    assert metadata['format'] == 'packed'
    assert metadata['file_size'] == 3 * loader.PACKED_SIZE


def test_preload_metadata_nonexistent(loader):
    """Test error handling for nonexistent file."""
    with pytest.raises(FileNotFoundError):
        loader.preload_metadata('/nonexistent/path/file.bin')


def test_preload_metadata_invalid_size(loader):
    """Test error handling for invalid file size."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as f:
        f.write(b'invalid')
        temp_path = f.name
    
    try:
        with pytest.raises(ValueError):
            loader.preload_metadata(temp_path)
    finally:
        Path(temp_path).unlink()


def test_load_packed(loader, temp_packed_file):
    """Test loading packed binary format."""
    file_path, expected_ticks = temp_packed_file
    
    loaded_ticks = list(loader.load(file_path, 'packed'))
    
    assert len(loaded_ticks) == 3
    
    for loaded, (tick_id, timestamp, price, volume) in zip(loaded_ticks, expected_ticks):
        assert loaded['tick_id'] == tick_id
        assert loaded['timestamp'] == timestamp
        assert loaded['price'] == price
        assert loaded['volume'] == volume


def test_load_binary(loader, temp_packed_file):
    """Test loading binary format (same structure as packed)."""
    file_path, expected_ticks = temp_packed_file
    
    loaded_ticks = list(loader.load(file_path, 'binary'))
    
    assert len(loaded_ticks) == 3
    
    for loaded, (tick_id, timestamp, price, volume) in zip(loaded_ticks, expected_ticks):
        assert loaded['tick_id'] == tick_id
        assert loaded['timestamp'] == timestamp
        assert loaded['price'] == price
        assert loaded['volume'] == volume


def test_load_unsupported_format(loader, temp_packed_file):
    """Test error handling for unsupported format."""
    file_path, _ = temp_packed_file
    
    with pytest.raises(ValueError):
        list(loader.load(file_path, 'unsupported'))


def test_load_nonexistent_file(loader):
    """Test error handling when loading nonexistent file."""
    with pytest.raises(FileNotFoundError):
        list(loader.load('/nonexistent/path/file.bin', 'packed'))


def test_load_streaming_behavior(loader, temp_packed_file):
    """Test that load returns iterator, not full list."""
    file_path, _ = temp_packed_file
    
    result = loader.load(file_path, 'packed')
    
    # Should be iterator, not list
    assert hasattr(result, '__iter__')
    assert hasattr(result, '__next__')


def test_load_negative_price(loader):
    """Test loading tick with negative price."""
    ticks = [
        (1, 1000000, -50000, 100),
    ]
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as f:
        for tick_id, timestamp, price, volume in ticks:
            data = struct.pack('>QQqQ', tick_id, timestamp, price, volume)
            f.write(data)
        temp_path = f.name
    
    try:
        loaded = list(loader.load(temp_path, 'packed'))
        assert len(loaded) == 1
        assert loaded[0]['price'] == -50000
    finally:
        Path(temp_path).unlink()


def test_load_large_values(loader):
    """Test loading ticks with maximum integer values."""
    max_unsigned = 2**64 - 1
    max_signed = 2**63 - 1
    
    ticks = [
        (max_unsigned, max_unsigned, max_signed, max_unsigned),
    ]
    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as f:
        for tick_id, timestamp, price, volume in ticks:
            data = struct.pack('>QQqQ', tick_id, timestamp, price, volume)
            f.write(data)
        temp_path = f.name
    
    try:
        loaded = list(loader.load(temp_path, 'packed'))
        assert len(loaded) == 1
        assert loaded[0]['tick_id'] == max_unsigned
        assert loaded[0]['timestamp'] == max_unsigned
        assert loaded[0]['price'] == max_signed
        assert loaded[0]['volume'] == max_unsigned
    finally:
        Path(temp_path).unlink()


def test_deterministic_load(loader):
    """Test that loading same file multiple times gives identical results."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.bin') as f:
        for i in range(5):
            data = struct.pack('>QQqQ', i, 1000000 + i * 10, 50000 + i, 100 + i)
            f.write(data)
        temp_path = f.name
    
    try:
        result1 = list(loader.load(temp_path, 'packed'))
        result2 = list(loader.load(temp_path, 'packed'))
        
        assert result1 == result2
    finally:
        Path(temp_path).unlink()
