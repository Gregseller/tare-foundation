"""
TARE snapshot_v1 module - Immutable snapshot structure for deterministic replay.
Phase 1: Define snapshot data structure with serialization/deserialization.
"""

import json
from pathlib import Path
from typing import Optional


class Snapshot:
    """
    Immutable snapshot structure for deterministic replay of tick data.
    
    Encapsulates:
    - List of tick data dictionaries with deterministic ordering
    - Metadata about snapshot creation and content
    - Serialization/deserialization for storage and retrieval
    - Binary search access to ticks by timestamp
    """

    def __init__(self, ticks: list[dict], metadata: dict) -> None:
        """
        Initialize immutable Snapshot from ticks and metadata.

        Args:
            ticks: List of tick dictionaries with fields:
                   - timestamp (int): Microseconds since epoch
                   - symbol (str): Asset symbol
                   - price (int): Price in base units
                   - volume (int): Volume in units
            metadata: Dict with snapshot information:
                     - version (str): Snapshot format version
                     - tick_count (int): Total ticks in snapshot
                     - timestamp_range (tuple): (min_ts, max_ts) in microseconds

        Raises:
            ValueError: If ticks or metadata invalid or ticks not sorted by timestamp.
            TypeError: If ticks not list[dict] or metadata not dict.
        """
        if not isinstance(ticks, list):
            raise TypeError("ticks must be a list")
        
        if not isinstance(metadata, dict):
            raise TypeError("metadata must be a dict")

        # Validate all ticks have required fields
        for i, tick in enumerate(ticks):
            if not isinstance(tick, dict):
                raise ValueError(f"tick {i} is not a dict")
            
            required_fields = {'timestamp', 'symbol', 'price', 'volume'}
            if not required_fields.issubset(set(tick.keys())):
                raise ValueError(
                    f"tick {i} missing required fields. Expected {required_fields}, "
                    f"got {set(tick.keys())}"
                )
            
            if not isinstance(tick['timestamp'], int):
                raise ValueError(f"tick {i} timestamp must be int, got {type(tick['timestamp'])}")
            
            if not isinstance(tick['symbol'], str):
                raise ValueError(f"tick {i} symbol must be str, got {type(tick['symbol'])}")
            
            if not isinstance(tick['price'], int):
                raise ValueError(f"tick {i} price must be int, got {type(tick['price'])}")
            
            if not isinstance(tick['volume'], int):
                raise ValueError(f"tick {i} volume must be int, got {type(tick['volume'])}")

        # Verify ticks are sorted by timestamp
        if len(ticks) > 1:
            for i in range(len(ticks) - 1):
                if ticks[i]['timestamp'] > ticks[i + 1]['timestamp']:
                    raise ValueError(
                        f"ticks not sorted by timestamp: tick {i} ({ticks[i]['timestamp']}) > "
                        f"tick {i+1} ({ticks[i+1]['timestamp']})"
                    )

        # Validate metadata
        required_metadata = {'version', 'tick_count', 'timestamp_range'}
        if not required_metadata.issubset(set(metadata.keys())):
            raise ValueError(
                f"metadata missing required fields. Expected {required_metadata}, "
                f"got {set(metadata.keys())}"
            )

        if not isinstance(metadata['version'], str):
            raise ValueError("metadata['version'] must be str")

        if not isinstance(metadata['tick_count'], int):
            raise ValueError("metadata['tick_count'] must be int")

        if metadata['tick_count'] != len(ticks):
            raise ValueError(
                f"metadata['tick_count'] ({metadata['tick_count']}) does not match "
                f"actual tick count ({len(ticks)})"
            )

        if not isinstance(metadata['timestamp_range'], (tuple, list)):
            raise ValueError("metadata['timestamp_range'] must be tuple or list")

        if len(metadata['timestamp_range']) != 2:
            raise ValueError("metadata['timestamp_range'] must have exactly 2 elements")

        min_ts, max_ts = metadata['timestamp_range']
        if not isinstance(min_ts, int) or not isinstance(max_ts, int):
            raise ValueError("timestamp_range elements must be int")

        if len(ticks) > 0:
            actual_min = ticks[0]['timestamp']
            actual_max = ticks[-1]['timestamp']

            if min_ts != actual_min or max_ts != actual_max:
                raise ValueError(
                    f"metadata timestamp_range ({min_ts}, {max_ts}) does not match "
                    f"actual range ({actual_min}, {actual_max})"
                )

        # Store immutable copies
        self._ticks = tuple(dict(tick) for tick in ticks)
        self._metadata = dict(metadata)

    def serialize(self, path: str) -> None:
        """
        Serialize snapshot to JSON file with deterministic ordering.

        Format:
        {
          "metadata": {...},
          "ticks": [...]
        }

        Args:
            path: File system path for output JSON file (str).

        Raises:
            ValueError: If path is invalid.
            IOError: If file write fails.
        """
        if not isinstance(path, str):
            raise ValueError("path must be a string")

        try:
            file_path = Path(path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Build serializable structure with sorted keys for determinism
            snapshot_data = {
                'metadata': dict(sorted(self._metadata.items())),
                'ticks': [dict(sorted(tick.items())) for tick in self._ticks]
            }

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(snapshot_data, f, separators=(',', ':'), indent=None)

        except IOError as e:
            raise IOError(f"Failed to write snapshot to {path}: {e}")

    @staticmethod
    def deserialize(path: str) -> 'Snapshot':
        """
        Deserialize snapshot from JSON file.

        Args:
            path: File system path to JSON snapshot file (str).

        Returns:
            Snapshot instance reconstructed from file.

        Raises:
            FileNotFoundError: If file does not exist.
            ValueError: If file format invalid or snapshot data invalid.
            IOError: If file read fails.
        """
        if not isinstance(path, str):
            raise ValueError("path must be a string")

        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"Snapshot file not found: {path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

        except IOError as e:
            raise IOError(f"Failed to read snapshot from {path}: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in snapshot file: {e}")

        if not isinstance(data, dict):
            raise ValueError("Snapshot file root must be a dict")

        if 'metadata' not in data or 'ticks' not in data:
            raise ValueError("Snapshot file missing 'metadata' or 'ticks' key")

        metadata = data['metadata']
        ticks = data['ticks']

        if not isinstance(metadata, dict):
            raise ValueError("Snapshot metadata must be a dict")

        if not isinstance(ticks, list):
            raise ValueError("Snapshot ticks must be a list")

        return Snapshot(ticks=ticks, metadata=metadata)

    def get_tick_at(self, time_us: int) -> dict:
        """
        Binary search for tick at or before given timestamp.

        Returns the tick with the largest timestamp that is <= time_us.
        If no such tick exists, raises ValueError.

        Args:
            time_us: Target timestamp in microseconds (int).

        Returns:
            Tick dictionary matching the search criteria.

        Raises:
            ValueError: If time_us invalid, before first tick, or not int.
        """
        if not isinstance(time_us, int):
            raise ValueError("time_us must be an int")

        if len(self._ticks) == 0:
            raise ValueError("Cannot search empty snapshot")

        if time_us < self._ticks[0]['timestamp']:
            raise ValueError(
                f"time_us ({time_us}) is before first tick "
                f"({self._ticks[0]['timestamp']})"
            )

        # Binary search for largest timestamp <= time_us
        left = 0
        right = len(self._ticks) - 1
        result_idx = 0

        while left <= right:
            mid = (left + right) // 2
            mid_ts = self._ticks[mid]['timestamp']

            if mid_ts <= time_us:
                result_idx = mid
                left = mid + 1
            else:
                right = mid - 1

        return dict(self._ticks[result_idx])

    def get_ticks(self) -> tuple[dict, ...]:
        """
        Get all ticks in snapshot as immutable tuple.

        Returns:
            Tuple of tick dictionaries in timestamp order.
        """
        return tuple(dict(tick) for tick in self._ticks)

    def get_metadata(self) -> dict:
        """
        Get snapshot metadata as copy.

        Returns:
            Dict with snapshot information.
        """
        return dict(self._metadata)

    def tick_count(self) -> int:
        """
        Get total number of ticks in snapshot.

        Returns:
            Count of ticks (int).
        """
        return len(self._ticks)

    def timestamp_range(self) -> tuple[int, int]:
        """
        Get min and max timestamps in snapshot.

        Returns:
            Tuple of (min_timestamp, max_timestamp) in microseconds.

        Raises:
            ValueError: If snapshot is empty.
        """
        if len(self._ticks) == 0:
            raise ValueError("Cannot get timestamp range of empty snapshot")

        return (self._ticks[0]['timestamp'], self._ticks[-1]['timestamp'])

    def stream_ticks(self):
        """
        Stream ticks one at a time without loading all into memory.

        Yields:
            Individual tick dictionaries in timestamp order.
        """
        for tick in self._ticks:
            yield dict(tick)

    def filter_by_symbol(self, symbol: str) -> 'Snapshot':
        """
        Create new snapshot containing only ticks for given symbol.

        Args:
            symbol: Asset symbol to filter by (str).

        Returns:
            New Snapshot instance with filtered ticks.

        Raises:
            ValueError: If symbol not found or invalid.
        """
        if not isinstance(symbol, str):
            raise ValueError("symbol must be a string")

        filtered_ticks = [tick for tick in self._ticks if tick['symbol'] == symbol]

        if len(filtered_ticks) == 0:
            raise ValueError(f"No ticks found for symbol '{symbol}'")

        # Create new metadata for filtered snapshot
        new_metadata = dict(self._metadata)
        new_metadata['tick_count'] = len(filtered_ticks)
        new_metadata['timestamp_range'] = (
            filtered_ticks[0]['timestamp'],
            filtered_ticks[-1]['timestamp']
        )

        return Snapshot(ticks=filtered_ticks, metadata=new_metadata)

    def __len__(self) -> int:
        """Get number of ticks in snapshot."""
        return len(self._ticks)

    def __getitem__(self, index: int) -> dict:
        """Get tick by index."""
        return dict(self._ticks[index])

    def __repr__(self) -> str:
        """String representation of snapshot."""
        return (
            f"Snapshot(version={self._metadata['version']!r}, "
            f"tick_count={len(self._ticks)}, "
            f"timestamp_range={self.timestamp_range()})"
        )
