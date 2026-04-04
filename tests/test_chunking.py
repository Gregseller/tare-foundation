"""
Unit tests for TARE Chunking module.

Tests deterministic chunking strategies with various input scenarios.
"""

import pytest
from tare.memory.chunking import Chunking


class TestChunkingBySize:
    """Tests for chunk_by_size method."""

    def test_basic_chunking(self):
        """Test basic fixed-size chunking."""
        ticks = [{"t": i, "p": 100 + i} for i in range(5)]
        result = Chunking.chunk_by_size(ticks, 2)
        assert len(result) == 3
        assert len(result[0]) == 2
        assert len(result[1]) == 2
        assert len(result[2]) == 1

    def test_chunk_size_equals_list_length(self):
        """Test when chunk size equals tick list length."""
        ticks = [{"t": i, "p": 100 + i} for i in range(3)]
        result = Chunking.chunk_by_size(ticks, 3)
        assert len(result) == 1
        assert result[0] == ticks

    def test_chunk_size_one(self):
        """Test chunking with size one."""
        ticks = [{"t": i, "p": 100 + i} for i in range(3)]
        result = Chunking.chunk_by_size(ticks, 1)
        assert len(result) == 3
        assert all(len(chunk) == 1 for chunk in result)

    def test_empty_list(self):
        """Test chunking empty tick list."""
        result = Chunking.chunk_by_size([], 5)
        assert result == []

    def test_single_tick(self):
        """Test chunking single tick."""
        ticks = [{"t": 0, "p": 100}]
        result = Chunking.chunk_by_size(ticks, 10)
        assert len(result) == 1
        assert result[0] == ticks

    def test_invalid_chunk_size_zero(self):
        """Test that zero chunk size raises ValueError."""
        with pytest.raises(ValueError, match="positive integer"):
            Chunking.chunk_by_size([{"t": 0}], 0)

    def test_invalid_chunk_size_negative(self):
        """Test that negative chunk size raises ValueError."""
        with pytest.raises(ValueError, match="positive integer"):
            Chunking.chunk_by_size([{"t": 0}], -5)

    def test_invalid_chunk_size_float(self):
        """Test that float chunk size raises ValueError."""
        with pytest.raises(ValueError, match="positive integer"):
            Chunking.chunk_by_size([{"t": 0}], 5.0)

    def test_deterministic_output(self):
        """Test that same input always produces same output."""
        ticks = [{"t": i, "p": 100 + i} for i in range(10)]
        result1 = Chunking.chunk_by_size(ticks, 3)
        result2 = Chunking.chunk_by_size(ticks, 3)
        assert result1 == result2

    def test_preserves_tick_data(self):
        """Test that chunking preserves all tick data."""
        ticks = [{"t": i, "p": 100 + i, "v": 1000 * i} for i in range(5)]
        result = Chunking.chunk_by_size(ticks, 2)
        flattened = [tick for chunk in result for tick in chunk]
        assert flattened == ticks


class TestChunkingByTime:
    """Tests for chunk_by_time method."""

    def test_basic_time_chunking(self):
        """Test basic time-window chunking."""
        ticks = [
            {"t": 0, "p": 100},
            {"t": 500, "p": 101},
            {"t": 2000, "p": 102},
        ]
        result = Chunking.chunk_by_time(ticks, 1000)
        assert len(result) == 2
        assert len(result[0]) == 2
        assert len(result[1]) == 1

    def test_all_ticks_in_single_window(self):
        """Test when all ticks fit in single time window."""
        ticks = [
            {"t": 0, "p": 100},
            {"t": 100, "p": 101},
            {"t": 200, "p": 102},
        ]
        result = Chunking.chunk_by_time(ticks, 1000)
        assert len(result) == 1
        assert result[0] == ticks

    def test_each_tick_separate_window(self):
        """Test when each tick creates separate window."""
        ticks = [
            {"t": 0, "p": 100},
            {"t": 1500, "p": 101},
            {"t": 3000, "p": 102},
        ]
        result = Chunking.chunk_by_time(ticks, 1000)
        assert len(result) == 3
        assert all(len(chunk) == 1 for chunk in result)

    def test_empty_tick_list(self):
        """Test chunking empty tick list."""
        result = Chunking.chunk_by_time([], 1000)
        assert result == []

    def test_single_tick(self):
        """Test chunking single tick."""
        ticks = [{"t": 0, "p": 100}]
        result = Chunking.chunk_by_time(ticks, 1000)
        assert len(result) == 1
        assert result[0] == ticks

    def test_exact_boundary_tick(self):
        """Test tick exactly at time window boundary."""
        ticks = [
            {"t": 0, "p": 100},
            {"t": 1000, "p": 101},
        ]
        result = Chunking.chunk_by_time(ticks, 1000)
        assert len(result) == 1
        assert len(result[0]) == 2

    def test_boundary_exceeded_tick(self):
        """Test tick just beyond time window boundary."""
        ticks = [
            {"t": 0, "p": 100},
            {"t": 1001, "p": 101},
        ]
        result = Chunking.chunk_by_time(ticks, 1000)
        assert len(result) == 2

    def test_invalid_time_window_zero(self):
        """Test that zero time window raises ValueError."""
        with pytest.raises(ValueError, match="positive integer"):
            Chunking.chunk_by_time([{"t": 0, "p": 100}], 0)

    def test_invalid_time_window_negative(self):
        """Test that negative time window raises ValueError."""
        with pytest.raises(ValueError, match="positive integer"):
            Chunking.chunk_by_time([{"t": 0, "p": 100}], -1000)

    def test_invalid_time_window_float(self):
        """Test that float time window raises ValueError."""
        with pytest.raises(ValueError, match="positive integer"):
            Chunking.chunk_by_time([{"t": 0, "p": 100}], 1000.0)

    def test_missing_timestamp_field(self):
        """Test that missing 't' field raises KeyError."""
        ticks = [{"p": 100}, {"t": 1000, "p": 101}]
        with pytest.raises(KeyError, match="'t'"):
            Chunking.chunk_by_time(ticks, 1000)

    def test_non_integer_timestamp(self):
        """Test that non-integer timestamp raises ValueError."""
        ticks = [{"t": 0.5, "p": 100}]
        with pytest.raises(ValueError, match="integer"):
            Chunking.chunk_by_time(ticks, 1000)

    def test_deterministic_output(self):
        """Test that same input always produces same output."""
        ticks = [
            {"t": i * 500, "p": 100 + i} for i in range(10)
        ]
        result1 = Chunking.chunk_by_time(ticks, 1000)
        result2 = Chunking.chunk_by_time(ticks, 1000)
        assert result1 == result2

    def test_preserves_tick_data(self):
        """Test that chunking preserves all tick data."""
        ticks = [
            {"t": i * 500, "p": 100 + i, "v": 1000 * i} for i in range(6)
        ]
        result = Chunking.chunk_by_time(ticks, 1500)
        flattened = [tick for chunk in result for tick in chunk]
        assert flattened == ticks

    def test_large_time_values(self):
        """Test with large timestamp values."""
        ticks = [
            {"t": 1000000000000, "p": 100},
            {"t": 1000000000500, "p": 101},
            {"t": 1000000002000, "p": 102},
        ]
        result = Chunking.chunk_by_time(ticks, 1000)
        assert len(result) == 2
        assert len(result[0]) == 2
        assert len(result[1]) == 1

    def test_unsorted_timestamps(self):
        """Test behavior with unsorted timestamps (time deltas calculated sequentially)."""
        ticks = [
            {"t": 1000, "p": 100},
            {"t": 500, "p": 101},
        ]
        result = Chunking.chunk_by_time(ticks, 1000)
        # t=500 is 500 microseconds before t=1000, but delta from window start is 500 - 1000 = -500
        # which is <= 1000, so both should be in same chunk
        assert len(result) == 1
