"""
TARE replay module - Deterministic historical tick stream replay from snapshot.
Phase 1: Replay tick data with exact timing control and seeking.
"""

from tare.snapshot.snapshot_v1 import Snapshot
from tare.time_engine.time_engine import TimeEngine


class Replay:
    """
    Replay historical tick stream from snapshot with exact timing.
    
    Provides deterministic, memory-efficient replay of tick data with support for:
    - Sequential tick iteration with correct timing
    - Seeking to specific timestamps
    - Status tracking (finished/current position)
    - Generator-based streaming for large datasets
    """

    def __init__(self, snapshot: Snapshot) -> None:
        """
        Initialize Replay with snapshot and time engine.

        Args:
            snapshot: Snapshot instance containing tick data to replay.

        Raises:
            TypeError: If snapshot is not a Snapshot instance.
            ValueError: If snapshot is empty.
        """
        if not isinstance(snapshot, Snapshot):
            raise TypeError("snapshot must be a Snapshot instance")
        
        if snapshot.tick_count() == 0:
            raise ValueError("Cannot replay empty snapshot")
        
        self._snapshot: Snapshot = snapshot
        self._time_engine: TimeEngine = TimeEngine(base_latency_ns=1_000)
        self._current_index: int = 0
        self._started: bool = False
        self._ticks: tuple = snapshot.get_ticks()

    def start(self) -> None:
        """
        Start replay from beginning of snapshot.
        
        Resets internal state and positions at first tick.
        
        Raises:
            ValueError: If snapshot is empty.
        """
        if len(self._ticks) == 0:
            raise ValueError("Cannot start replay of empty snapshot")
        
        self._current_index = 0
        self._time_engine.reset()
        self._started = True

    def next_tick(self) -> dict:
        """
        Get next tick from replay stream with timing information.
        
        Returns merged dict containing:
        - Original tick fields: timestamp, symbol, price, volume
        - Timing fields: simulation_time, sequence, local_time
        
        Returns:
            Dict with tick data and timing information.
        
        Raises:
            ValueError: If replay not started or reached end.
        """
        if not self._started:
            raise ValueError("Replay not started. Call start() first.")
        
        if self._current_index >= len(self._ticks):
            raise ValueError("Replay finished. No more ticks available.")
        
        current_tick: dict = dict(self._ticks[self._current_index])
        market_time_us: int = current_tick['timestamp']
        
        # Convert microseconds to nanoseconds for time engine
        market_time_ns: int = market_time_us * 1_000
        
        # Get timing information from time engine
        timing_info: dict = self._time_engine.process_event(market_time_ns)
        
        # Merge tick data with timing information
        result: dict = dict(current_tick)
        result['simulation_time'] = timing_info['simulation_time']
        result['sequence'] = timing_info['sequence']
        result['local_time'] = timing_info['local_time'] // 1_000  # Convert back to microseconds
        
        self._current_index += 1
        
        return result

    def seek(self, time_us: int) -> None:
        """
        Seek replay to position at or before given timestamp.
        
        Performs binary search to find tick at or before time_us,
        resets time engine, and positions replay at that tick.
        
        Args:
            time_us: Target timestamp in microseconds (int).
        
        Raises:
            TypeError: If time_us is not int.
            ValueError: If time_us before first tick or seek invalid.
        """
        if not isinstance(time_us, int):
            raise TypeError("time_us must be an int")
        
        if len(self._ticks) == 0:
            raise ValueError("Cannot seek in empty snapshot")
        
        if time_us < self._ticks[0]['timestamp']:
            raise ValueError(
                f"time_us ({time_us}) is before first tick "
                f"({self._ticks[0]['timestamp']})"
            )
        
        # Binary search for position at or before time_us
        left: int = 0
        right: int = len(self._ticks) - 1
        result_idx: int = 0
        
        while left <= right:
            mid: int = (left + right) // 2
            mid_ts: int = self._ticks[mid]['timestamp']
            
            if mid_ts <= time_us:
                result_idx = mid
                left = mid + 1
            else:
                right = mid - 1
        
        # Reset time engine and replay all ticks up to seek position
        self._time_engine.reset()
        
        # Process all ticks up to and including result_idx to sync time engine
        for i in range(result_idx + 1):
            tick_ns: int = self._ticks[i]['timestamp'] * 1_000
            self._time_engine.process_event(tick_ns)
        
        self._current_index = result_idx + 1
        self._started = True

    def is_finished(self) -> bool:
        """
        Check if replay has reached end of snapshot.
        
        Returns:
            True if all ticks have been consumed, False otherwise.
        """
        return self._current_index >= len(self._ticks)

    def _find_tick_index(self, time_us: int) -> int:
        """
        Binary search helper to find tick index at or before timestamp.
        
        Args:
            time_us: Target timestamp in microseconds (int).
        
        Returns:
            Index of tick at or before time_us (int).
        
        Raises:
            ValueError: If time_us before first tick.
        """
        if time_us < self._ticks[0]['timestamp']:
            raise ValueError(
                f"time_us ({time_us}) is before first tick "
                f"({self._ticks[0]['timestamp']})"
            )
        
        left: int = 0
        right: int = len(self._ticks) - 1
        result_idx: int = 0
        
        while left <= right:
            mid: int = (left + right) // 2
            mid_ts: int = self._ticks[mid]['timestamp']
            
            if mid_ts <= time_us:
                result_idx = mid
                left = mid + 1
            else:
                right = mid - 1
        
        return result_idx

    def get_position(self) -> int:
        """
        Get current position in replay (index of next tick to be read).
        
        Returns:
            Current index in tick stream (int).
        """
        return self._current_index

    def get_total_ticks(self) -> int:
        """
        Get total number of ticks in snapshot.
        
        Returns:
            Total tick count (int).
        """
        return len(self._ticks)

    def __repr__(self) -> str:
        """String representation of Replay state."""
        return (
            f"Replay(position={self._current_index}/{len(self._ticks)}, "
            f"started={self._started}, "
            f"finished={self.is_finished()})"
        )
