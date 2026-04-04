"""
jitter_corrector.py — JitterCorrector v1
TARE (Tick-Level Algorithmic Research Environment)

Correct timing artifacts and synchronize multi-source ticks.
Phase 1: Core jitter correction with deterministic processing.
"""

from typing import Generator, Optional
from tare.time_engine.time_engine import TimeEngine
from tare.tick_data_engine.tick_cleaner import TickCleaner


class JitterCorrector:
    """
    Correct timing artifacts in tick data and synchronize ticks from multiple sources.

    The module provides two main functions:
    1. correct_timestamps: Smooth timestamps within a single source by removing
       jitter smaller than a specified threshold.
    2. synchronize_sources: Merge ticks from multiple sources into a single
       time-ordered stream, using TimeEngine to generate deterministic simulation time.

    Rules:
      - Only int, no float anywhere
      - No randomness
      - Deterministic: same input → same output
      - Use generators for data streams when possible
    """

    def __init__(self, time_engine: Optional[TimeEngine] = None, max_jitter_us: int = 1000):
        """
        Initialize JitterCorrector.

        Args:
            time_engine: Optional TimeEngine instance. If not provided,
                        a new one will be created with default latency.
        """
        self.time_engine = time_engine if time_engine else TimeEngine()
        self.tick_cleaner = TickCleaner()
        self.max_jitter_us = max_jitter_us

    def correct_timestamps(self, ticks: list[dict], max_jitter_us: int) -> list[dict]:
        """
        Correct timing artifacts by smoothing timestamps within a single source.

        Algorithm:
        1. Sort ticks by original timestamp (ascending)
        2. For each tick, if its timestamp is within max_jitter_us of the
           previous corrected timestamp, use the previous corrected timestamp
        3. Otherwise, use the original timestamp
        4. All timestamps are in nanoseconds, max_jitter_us is converted to ns

        Args:
            ticks: List of tick dictionaries. Each tick must have 'timestamp' key.
            max_jitter_us: Maximum jitter to correct in microseconds.
                          Must be non-negative int.

        Returns:
            List of tick dictionaries with corrected timestamps.
            Original tick data is preserved, only 'timestamp' is modified.

        Raises:
            ValueError: If max_jitter_us is negative or ticks are invalid.
        """
        if not isinstance(max_jitter_us, int) or max_jitter_us < 0:
            raise ValueError("max_jitter_us must be non-negative int")

        if not isinstance(ticks, list):
            raise ValueError("ticks must be a list")

        if not ticks:
            return []

        # Clean ticks first to ensure valid data
        cleaned_ticks = self.tick_cleaner.clean(ticks)

        # Sort by original timestamp
        sorted_ticks = sorted(cleaned_ticks, key=lambda x: x['timestamp'])

        # Convert max_jitter_us to nanoseconds (1us = 1000ns)
        max_jitter_ns = max_jitter_us * 1000

        corrected_ticks = []
        last_corrected_ts = None

        for tick in sorted_ticks:
            current_ts = tick['timestamp']

            if last_corrected_ts is None:
                # First tick, use original timestamp
                corrected_ts = current_ts
            else:
                # Check if within jitter threshold
                if abs(current_ts - last_corrected_ts) < max_jitter_ns:
                    # Within jitter — snap to previous timestamp
                    corrected_ts = last_corrected_ts
                else:
                    # Outside jitter — keep original
                    corrected_ts = current_ts

            # Create new tick with corrected timestamp
            corrected_tick = tick.copy()
            corrected_tick['timestamp'] = corrected_ts
            corrected_ticks.append(corrected_tick)

            last_corrected_ts = corrected_ts

        return corrected_ticks

    def synchronize_sources(self, ticks_by_source: dict[str, list[dict]]) -> list[dict]:
        """
        Synchronize ticks from multiple sources into a single time-ordered stream.

        Algorithm:
        1. Clean ticks from each source
        2. Apply jitter correction to each source (using default 10us threshold)
        3. Merge all ticks into a single list
        4. Sort by corrected timestamp
        5. Process through TimeEngine to generate deterministic simulation time

        Args:
            ticks_by_source: Dictionary mapping source names to lists of ticks.
                            Example: {'source1': [tick1, tick2], 'source2': [tick3]}

        Returns:
            List of synchronized tick dictionaries with additional metadata:
            - All original tick fields
            - 'source': source identifier
            - 'market_time': original timestamp (renamed from 'timestamp')
            - 'local_time': timestamp after latency
            - 'simulation_time': deterministic monotonic counter
            - 'sequence': event sequence number

        Raises:
            ValueError: If ticks_by_source is not a dict or contains invalid data.
        """
        if not isinstance(ticks_by_source, dict):
            raise ValueError("ticks_by_source must be a dictionary")

        all_ticks = []

        for source_name, source_ticks in ticks_by_source.items():
            if not isinstance(source_name, str):
                raise ValueError("Source names must be strings")

            if not isinstance(source_ticks, list):
                raise ValueError(f"Ticks for source '{source_name}' must be a list")

            # Save original timestamps before jitter correction
            cleaned = self.tick_cleaner.clean(source_ticks)
            original_timestamps = [t['timestamp'] for t in cleaned]
            # Apply jitter correction for ordering
            corrected_ticks = self.correct_timestamps(source_ticks, max_jitter_us=10)

            # market_time = ORIGINAL timestamp (before jitter correction)
            for i, tick in enumerate(corrected_ticks):
                synchronized_tick = tick.copy()
                synchronized_tick['source'] = source_name
                synchronized_tick['market_time'] = original_timestamps[i]
                synchronized_tick.pop('timestamp', None)
                all_ticks.append(synchronized_tick)

        # Sort all ticks by market_time (ascending)
        sorted_ticks = sorted(all_ticks, key=lambda x: x['market_time'])

        # Process through TimeEngine for deterministic timing
        synchronized_result = []

        for tick in sorted_ticks:
            # Process event through TimeEngine
            time_event = self.time_engine.process_event(tick['market_time'])

            # Merge tick data with time event data
            final_tick = tick.copy()
            final_tick.update(time_event)
            synchronized_result.append(final_tick)

        return synchronized_result

    def correct_timestamps_generator(
        self,
        ticks: Generator[dict, None, None],
        max_jitter_us: int
    ) -> Generator[dict, None, None]:
        """
        Correct timestamps for a generator of ticks.

        This method processes ticks one by one to avoid loading all data
        into memory. Note that jitter correction requires maintaining state
        about previous corrected timestamp.

        Args:
            ticks: Generator yielding tick dictionaries.
            max_jitter_us: Maximum jitter to correct in microseconds.

        Yields:
            Tick dictionaries with corrected timestamps.
        """
        if not isinstance(max_jitter_us, int) or max_jitter_us < 0:
            raise ValueError("max_jitter_us must be non-negative int")

        max_jitter_ns = max_jitter_us * 1000
        last_corrected_ts = None

        # We need to sort ticks for jitter correction, so we collect them first
        # This is a limitation of the algorithm when using generators
        collected_ticks = []

        for tick in ticks:
            # Basic validation
            if not isinstance(tick, dict):
                continue

            if 'timestamp' not in tick or not isinstance(tick['timestamp'], int):
                continue

            collected_ticks.append(tick)

        # Sort by timestamp
        sorted_ticks = sorted(collected_ticks, key=lambda x: x['timestamp'])

        # Apply jitter correction
        for tick in sorted_ticks:
            current_ts = tick['timestamp']

            if last_corrected_ts is None:
                corrected_ts = current_ts
            else:
                if abs(current_ts - last_corrected_ts) < max_jitter_ns:
                    corrected_ts = last_corrected_ts
                else:
                    corrected_ts = current_ts

            corrected_tick = tick.copy()
            corrected_tick['timestamp'] = corrected_ts
            yield corrected_tick

            last_corrected_ts = corrected_ts
