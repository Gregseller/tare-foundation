"""
Tests for tare.snapshot.replay module.
Validates deterministic tick replay with timing, seeking, and state management.
"""

import pytest
from tare.snapshot.snapshot_v1 import Snapshot
from tare.snapshot.replay import Replay


class TestReplayInit:
    """Tests for Replay initialization."""

    def test_init_valid_snapshot(self) -> None:
        """Test initialization with valid snapshot."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1_000_000, 2_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        assert replay.get_position() == 0
        assert replay.get_total_ticks() == 2
        assert not replay.is_finished()

    def test_init_not_started(self) -> None:
        """Test that replay is not started after init."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1_000_000, 1_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        with pytest.raises(ValueError, match="Replay not started"):
            replay.next_tick()

    def test_init_empty_snapshot_raises(self) -> None:
        """Test that empty snapshot raises ValueError."""
        metadata = {
            'version': '1.0',
            'tick_count': 0,
            'timestamp_range': (0, 0)
        }
        snapshot = Snapshot(ticks=[], metadata=metadata)
        
        with pytest.raises(ValueError, match="Cannot replay empty snapshot"):
            Replay(snapshot)

    def test_init_invalid_type_raises(self) -> None:
        """Test that non-Snapshot argument raises TypeError."""
        with pytest.raises(TypeError, match="snapshot must be a Snapshot instance"):
            Replay(snapshot="not a snapshot")
        
        with pytest.raises(TypeError, match="snapshot must be a Snapshot instance"):
            Replay(snapshot=None)
        
        with pytest.raises(TypeError, match="snapshot must be a Snapshot instance"):
            Replay(snapshot={})


class TestReplayStart:
    """Tests for Replay.start() method."""

    def test_start_resets_position(self) -> None:
        """Test that start() resets position to 0."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
            {'timestamp': 3_000_000, 'symbol': 'BTC', 'price': 50_200, 'volume': 3},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 3_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        replay.start()
        assert replay.get_position() == 0
        
        # Advance position
        replay.next_tick()
        replay.next_tick()
        assert replay.get_position() == 2
        
        # Start again - position should reset
        replay.start()
        assert replay.get_position() == 0

    def test_start_allows_next_tick(self) -> None:
        """Test that start() enables next_tick() calls."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1_000_000, 1_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        replay.start()
        tick = replay.next_tick()
        assert tick['symbol'] == 'BTC'


class TestReplayNextTick:
    """Tests for Replay.next_tick() method."""

    def test_next_tick_returns_tick_data(self) -> None:
        """Test that next_tick returns correct tick fields."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1_000_000, 1_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        replay.start()
        
        tick = replay.next_tick()
        assert tick['timestamp'] == 1_000_000
        assert tick['symbol'] == 'BTC'
        assert tick['price'] == 50_000
        assert tick['volume'] == 1

    def test_next_tick_adds_timing_fields(self) -> None:
        """Test that next_tick adds simulation_time, sequence, local_time."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1_000_000, 2_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        replay.start()
        
        tick1 = replay.next_tick()
        assert 'simulation_time' in tick1
        assert 'sequence' in tick1
        assert 'local_time' in tick1
        assert isinstance(tick1['simulation_time'], int)
        assert isinstance(tick1['sequence'], int)
        assert isinstance(tick1['local_time'], int)

    def test_next_tick_increments_sequence(self) -> None:
        """Test that sequence increments with each tick."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
            {'timestamp': 3_000_000, 'symbol': 'BTC', 'price': 50_200, 'volume': 3},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 3_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        replay.start()
        
        tick1 = replay.next_tick()
        tick2 = replay.next_tick()
        tick3 = replay.next_tick()
        
        assert tick1['sequence'] == 1
        assert tick2['sequence'] == 2
        assert tick3['sequence'] == 3

    def test_next_tick_increments_simulation_time(self) -> None:
        """Test that simulation_time increments monotonically."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 2},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 3},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 2_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        replay.start()
        
        tick1 = replay.next_tick()
        tick2 = replay.next_tick()
        tick3 = replay.next_tick()
        
        sim_times = [tick1['simulation_time'], tick2['simulation_time'], tick3['simulation_time']]
        assert sim_times == sorted(sim_times)
        assert len(set(sim_times)) == 3

    def test_next_tick_exhaustion_raises(self) -> None:
        """Test that next_tick raises when all ticks consumed."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1_000_000, 1_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        replay.start()
        
        replay.next_tick()
        with pytest.raises(ValueError, match="Replay finished"):
            replay.next_tick()

    def test_next_tick_not_started_raises(self) -> None:
        """Test that next_tick raises if start() not called."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1_000_000, 1_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        with pytest.raises(ValueError, match="Replay not started"):
            replay.next_tick()


class TestReplaySeek:
    """Tests for Replay.seek() method."""

    def test_seek_to_first_tick(self) -> None:
        """Test seeking to first tick."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
            {'timestamp': 3_000_000, 'symbol': 'BTC', 'price': 50_200, 'volume': 3},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 3_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        replay.seek(1_000_000)
        tick = replay.next_tick()
        assert tick['timestamp'] == 2_000_000

    def test_seek_to_middle_tick(self) -> None:
        """Test seeking to middle tick."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
            {'timestamp': 3_000_000, 'symbol': 'BTC', 'price': 50_200, 'volume': 3},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 3_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        replay.seek(2_000_000)
        tick = replay.next_tick()
        assert tick['timestamp'] == 3_000_000

    def test_seek_between_ticks(self) -> None:
        """Test seeking to time between two ticks."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
            {'timestamp': 3_000_000, 'symbol': 'BTC', 'price': 50_200, 'volume': 3},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 3_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        # Seek to 1_500_000 (between tick 1 and 2)
        replay.seek(1_500_000)
        tick = replay.next_tick()
        assert tick['timestamp'] == 2_000_000

    def test_seek_to_last_tick(self) -> None:
        """Test seeking to last tick."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
            {'timestamp': 3_000_000, 'symbol': 'BTC', 'price': 50_200, 'volume': 3},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 3_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        replay.seek(3_000_000)
        assert replay.is_finished()

    def test_seek_before_first_tick_raises(self) -> None:
        """Test that seeking before first tick raises ValueError."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1_000_000, 2_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        with pytest.raises(ValueError, match="is before first tick"):
            replay.seek(999_999)

    def test_seek_invalid_type_raises(self) -> None:
        """Test that non-int time_us raises TypeError."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 1,
            'timestamp_range': (1_000_000, 1_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        with pytest.raises(TypeError, match="time_us must be an int"):
            replay.seek("1000000")
        
        with pytest.raises(TypeError, match="time_us must be an int"):
            replay.seek(1000000.5)

    def test_seek_resets_time_engine(self) -> None:
        """Test that seek properly resets time engine state."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
            {'timestamp': 3_000_000, 'symbol': 'BTC', 'price': 50_200, 'volume': 3},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 3_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        # Start from beginning
        replay.start()
        tick1 = replay.next_tick()
        tick2 = replay.next_tick()
        seq_after_normal = tick2['sequence']
        
        # Seek and get next
        replay.seek(1_000_000)
        tick_after_seek = replay.next_tick()
        seq_after_seek = tick_after_seek['sequence']
        
        # Sequence should start from 2 (first tick is at index 0, seek positions at 1)
        assert seq_after_seek == 2


class TestReplayIsFinished:
    """Tests for Replay.is_finished() method."""

    def test_is_finished_initially_false(self) -> None:
        """Test that is_finished is False after start()."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1_000_000, 2_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        replay.start()
        
        assert not replay.is_finished()

    def test_is_finished_after_all_ticks(self) -> None:
        """Test that is_finished is True after consuming all ticks."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1_000_000, 2_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        replay.start()
        
        replay.next_tick()
        assert not replay.is_finished()
        
        replay.next_tick()
        assert replay.is_finished()

    def test_is_finished_after_seek_to_end(self) -> None:
        """Test that is_finished is True after seeking to last tick."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1_000_000, 2_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        replay.seek(2_000_000)
        assert replay.is_finished()


class TestReplayDeterminism:
    """Tests for deterministic behavior."""

    def test_deterministic_replay_sequence(self) -> None:
        """Test that same snapshot replayed twice produces identical results."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
            {'timestamp': 3_000_000, 'symbol': 'BTC', 'price': 50_200, 'volume': 3},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 3_000_000)
        }
        
        # First replay
        snapshot1 = Snapshot(ticks=ticks, metadata=metadata)
        replay1 = Replay(snapshot1)
        replay1.start()
        result1 = [replay1.next_tick() for _ in range(3)]
        
        # Second replay
        snapshot2 = Snapshot(ticks=ticks, metadata=metadata)
        replay2 = Replay(snapshot2)
        replay2.start()
        result2 = [replay2.next_tick() for _ in range(3)]
        
        assert result1 == result2

    def test_deterministic_seek_and_replay(self) -> None:
        """Test that seek produces deterministic results."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
            {'timestamp': 3_000_000, 'symbol': 'BTC', 'price': 50_200, 'volume': 3},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 3_000_000)
        }
        
        # First seek
        snapshot1 = Snapshot(ticks=ticks, metadata=metadata)
        replay1 = Replay(snapshot1)
        replay1.seek(1_500_000)
        result1 = [replay1.next_tick() for _ in range(2)]
        
        # Second seek
        snapshot2 = Snapshot(ticks=ticks, metadata=metadata)
        replay2 = Replay(snapshot2)
        replay2.seek(1_500_000)
        result2 = [replay2.next_tick() for _ in range(2)]
        
        assert result1 == result2


class TestReplayIntegration:
    """Integration tests for Replay."""

    def test_full_replay_workflow(self) -> None:
        """Test complete replay workflow: start, iterate, finish."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'ETH', 'price': 3_000, 'volume': 10},
            {'timestamp': 3_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 3,
            'timestamp_range': (1_000_000, 3_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        
        replay.start()
        all_ticks = []
        while not replay.is_finished():
            all_ticks.append(replay.next_tick())
        
        assert len(all_ticks) == 3
        assert all_ticks[0]['symbol'] == 'BTC'
        assert all_ticks[1]['symbol'] == 'ETH'
        assert all_ticks[2]['symbol'] == 'BTC'

    def test_replay_all_values_int(self) -> None:
        """Test that all timing values are integers, no floats."""
        ticks = [
            {'timestamp': 1_000_000, 'symbol': 'BTC', 'price': 50_000, 'volume': 1},
            {'timestamp': 2_000_000, 'symbol': 'BTC', 'price': 50_100, 'volume': 2},
        ]
        metadata = {
            'version': '1.0',
            'tick_count': 2,
            'timestamp_range': (1_000_000, 2_000_000)
        }
        snapshot = Snapshot(ticks=ticks, metadata=metadata)
        replay = Replay(snapshot)
        replay.start()
        
        tick = replay.next_tick()
        for key, value in tick.items():
            assert isinstance(value, (int, str)), f"Field {key} has non-int/str value: {type(value)}"
