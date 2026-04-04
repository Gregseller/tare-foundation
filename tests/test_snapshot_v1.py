"""
Tests for TARE snapshot_v1 module.
"""

import json
import tempfile
from pathlib import Path

import pytest

from tare.snapshot.snapshot_v1 import Snapshot


class TestSnapshotInit:
    """Test Snapshot initialization and validation."""

    def test_valid_snapshot_creation(self):
        """Test creating valid snapshot with correct data."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'BTC', 'price': 50100, 'volume': 150},
            {'timestamp': 3000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1000, 3000)
        }

        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        assert snapshot.tick_count() == 3
        assert snapshot.timestamp_range() == (1000, 3000)

    def test_empty_snapshot(self):
        """Test creating snapshot with no ticks."""
        ticks = []
        metadata = {
            'version': '1.0',
            'tick_count': 0,
            'timestamp_range': (0, 0)
        }

        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        assert snapshot.tick_count() == 0

    def test_ticks_not_list_raises_error(self):
        """Test that non-list ticks raises TypeError."""
        with pytest.raises(TypeError, match="ticks must be a list"):
            Snapshot(ticks="not a list", metadata={})

    def test_metadata_not_dict_raises_error(self):
        """Test that non-dict metadata raises TypeError."""
        with pytest.raises(TypeError, match="metadata must be a dict"):
            Snapshot(ticks=[], metadata="not a dict")

    def test_tick_missing_required_fields(self):
        """Test that tick missing required fields raises ValueError."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000}  # missing volume
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1000, 1000)
        }

        with pytest.raises(ValueError, match="tick 0 missing required fields"):
            Snapshot(ticks=ticks, metadata=metadata)

    def test_tick_invalid_timestamp_type(self):
        """Test that non-int timestamp raises ValueError."""
        ticks = [
            {'timestamp': 1000.5, 'symbol': 'BTC', 'price': 50000, 'volume': 100}
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1000, 1000)
        }

        with pytest.raises(ValueError, match="timestamp must be int"):
            Snapshot(ticks=ticks, metadata=metadata)

    def test_tick_invalid_price_type(self):
        """Test that non-int price raises ValueError."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000.5, 'volume': 100}
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1000, 1000)
        }

        with pytest.raises(ValueError, match="price must be int"):
            Snapshot(ticks=ticks, metadata=metadata)

    def test_ticks_not_sorted_by_timestamp(self):
        """Test that unsorted ticks raise ValueError."""
        ticks = [
            {'timestamp': 3000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50100, 'volume': 150},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1000, 3000)
        }

        with pytest.raises(ValueError, match="ticks not sorted by timestamp"):
            Snapshot(ticks=ticks, metadata=metadata)

    def test_metadata_missing_required_fields(self):
        """Test that metadata missing required fields raises ValueError."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100}
        ]
        metadata = {'version': '1.0'}  # missing tick_count and timestamp_range

        with pytest.raises(ValueError, match="metadata missing required fields"):
            Snapshot(ticks=ticks, metadata=metadata)

    def test_metadata_tick_count_mismatch(self):
        """Test that mismatched tick_count raises ValueError."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100}
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 999,  # mismatch
            'timestamp_range': (1000, 1000)
        }

        with pytest.raises(ValueError, match="tick_count.*does not match"):
            Snapshot(ticks=ticks, metadata=metadata)

    def test_metadata_timestamp_range_mismatch(self):
        """Test that mismatched timestamp_range raises ValueError."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'BTC', 'price': 50100, 'volume': 150},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (999, 2001)  # mismatch
        }

        with pytest.raises(ValueError, match="timestamp_range.*does not match"):
            Snapshot(ticks=ticks, metadata=metadata)


class TestSnapshotSerialization:
    """Test snapshot serialization and deserialization."""

    def test_serialize_to_json(self):
        """Test serializing snapshot to JSON file."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1000, 2000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(Path(tmpdir) / 'snapshot.json')
            snapshot.serialize(path)

            assert Path(path).exists()
            with open(path, 'r') as f:
                data = json.load(f)

            assert 'metadata' in data
            assert 'ticks' in data
            assert len(data['ticks']) == 2

    def test_serialize_path_not_string_raises_error(self):
        """Test that non-string path raises ValueError."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100}
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1000, 1000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        with pytest.raises(ValueError, match="path must be a string"):
            snapshot.serialize(123)

    def test_deserialize_from_json(self):
        """Test deserializing snapshot from JSON file."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1000, 2000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(Path(tmpdir) / 'snapshot.json')
            snapshot.serialize(path)

            loaded = Snapshot.deserialize(path)
            assert loaded.tick_count() == 2
            assert loaded.timestamp_range() == (1000, 2000)

    def test_deserialize_file_not_found_raises_error(self):
        """Test that deserializing nonexistent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            Snapshot.deserialize('/nonexistent/path.json')

    def test_deserialize_invalid_json_raises_error(self):
        """Test that invalid JSON raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(Path(tmpdir) / 'invalid.json')
            with open(path, 'w') as f:
                f.write('invalid json content {{{')

            with pytest.raises(ValueError, match="Invalid JSON"):
                Snapshot.deserialize(path)

    def test_deserialize_missing_metadata_raises_error(self):
        """Test that missing metadata raises ValueError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(Path(tmpdir) / 'bad.json')
            with open(path, 'w') as f:
                json.dump({'ticks': []}, f)

            with pytest.raises(ValueError, match="missing 'metadata' or 'ticks'"):
                Snapshot.deserialize(path)

    def test_roundtrip_serialization(self):
        """Test that serialize-deserialize roundtrip preserves data."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
            {'timestamp': 3000, 'symbol': 'BTC', 'price': 50100, 'volume': 150},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1000, 3000)
        }
        original = Snapshot(ticks=ticks, metadata=metadata)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = str(Path(tmpdir) / 'snapshot.json')
            original.serialize(path)
            loaded = Snapshot.deserialize(path)

            assert loaded.tick_count() == original.tick_count()
            assert loaded.timestamp_range() == original.timestamp_range()
            assert list(loaded.get_ticks()) == list(original.get_ticks())


class TestSnapshotGetTickAt:
    """Test binary search for tick by timestamp."""

    def test_get_tick_at_exact_match(self):
        """Test finding tick with exact timestamp match."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'BTC', 'price': 50100, 'volume': 150},
            {'timestamp': 3000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1000, 3000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        tick = snapshot.get_tick_at(2000)
        assert tick['timestamp'] == 2000
        assert tick['symbol'] == 'BTC'

    def test_get_tick_at_between_timestamps(self):
        """Test finding nearest tick before given timestamp."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'BTC', 'price': 50100, 'volume': 150},
            {'timestamp': 3000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1000, 3000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        tick = snapshot.get_tick_at(2500)
        assert tick['timestamp'] == 2000  # nearest before 2500

    def test_get_tick_at_first_tick(self):
        """Test getting the first tick."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'BTC', 'price': 50100, 'volume': 150},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1000, 2000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        tick = snapshot.get_tick_at(1000)
        assert tick['timestamp'] == 1000

    def test_get_tick_at_last_tick(self):
        """Test getting the last tick."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'BTC', 'price': 50100, 'volume': 150},
            {'timestamp': 3000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1000, 3000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        tick = snapshot.get_tick_at(3000)
        assert tick['timestamp'] == 3000

    def test_get_tick_at_before_first_raises_error(self):
        """Test that timestamp before first tick raises ValueError."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100}
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1000, 1000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        with pytest.raises(ValueError, match="before first tick"):
            snapshot.get_tick_at(999)

    def test_get_tick_at_non_int_raises_error(self):
        """Test that non-int timestamp raises ValueError."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100}
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1000, 1000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        with pytest.raises(ValueError, match="must be an int"):
            snapshot.get_tick_at(1000.5)

    def test_get_tick_at_empty_snapshot_raises_error(self):
        """Test that searching empty snapshot raises ValueError."""
        metadata = {
            'version': '1.0',
            'tick_count': 0,
            'timestamp_range': (0, 0)
        }
        snapshot = Snapshot(ticks=[], metadata=metadata)

        with pytest.raises(ValueError, match="Cannot search empty snapshot"):
            snapshot.get_tick_at(1000)


class TestSnapshotAccessors:
    """Test snapshot accessor methods."""

    def test_get_ticks(self):
        """Test retrieving all ticks."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1000, 2000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        result = snapshot.get_ticks()
        assert len(result) == 2
        assert result[0]['timestamp'] == 1000
        assert result[1]['timestamp'] == 2000

    def test_get_metadata(self):
        """Test retrieving snapshot metadata."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100}
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1000, 1000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        result = snapshot.get_metadata()
        assert result['version'] == '1.0'
        assert result['tick_count'] == 1

    def test_tick_count(self):
        """Test getting tick count."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1000, 2000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        assert snapshot.tick_count() == 2

    def test_timestamp_range(self):
        """Test getting timestamp range."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 5000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1000, 5000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        assert snapshot.timestamp_range() == (1000, 5000)

    def test_timestamp_range_empty_snapshot_raises_error(self):
        """Test that empty snapshot raises ValueError."""
        metadata = {
            'version': '1.0',
            'tick_count': 0,
            'timestamp_range': (0, 0)
        }
        snapshot = Snapshot(ticks=[], metadata=metadata)

        with pytest.raises(ValueError, match="empty snapshot"):
            snapshot.timestamp_range()


class TestSnapshotStream:
    """Test streaming ticks from snapshot."""

    def test_stream_ticks(self):
        """Test streaming ticks one at a time."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1000, 2000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        streamed = list(snapshot.stream_ticks())
        assert len(streamed) == 2
        assert streamed[0]['timestamp'] == 1000
        assert streamed[1]['timestamp'] == 2000


class TestSnapshotFilter:
    """Test filtering snapshot by symbol."""

    def test_filter_by_symbol(self):
        """Test filtering snapshot for single symbol."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
            {'timestamp': 3000, 'symbol': 'BTC', 'price': 50100, 'volume': 150},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1000, 3000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        filtered = snapshot.filter_by_symbol('BTC')
        assert filtered.tick_count() == 2
        assert filtered.get_ticks()[0]['symbol'] == 'BTC'
        assert filtered.get_ticks()[1]['symbol'] == 'BTC'

    def test_filter_by_nonexistent_symbol_raises_error(self):
        """Test that filtering nonexistent symbol raises ValueError."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100}
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1000, 1000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        with pytest.raises(ValueError, match="No ticks found"):
            snapshot.filter_by_symbol('ETH')


class TestSnapshotIndexing:
    """Test indexing and length operations."""

    def test_len(self):
        """Test len() on snapshot."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1000, 2000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        assert len(snapshot) == 2

    def test_getitem(self):
        """Test indexing with []."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'ETH', 'price': 3000, 'volume': 200},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1000, 2000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        tick = snapshot[0]
        assert tick['timestamp'] == 1000
        assert tick['symbol'] == 'BTC'

    def test_repr(self):
        """Test string representation."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'BTC', 'price': 50000, 'volume': 100}
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1000, 1000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)

        repr_str = repr(snapshot)
        assert 'Snapshot' in repr_str
        assert '1.0' in repr_str
        assert '1' in repr_str
