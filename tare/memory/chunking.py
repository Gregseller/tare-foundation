"""
TARE Chunking Module - Memory-friendly tick stream segmentation.

Splits large tick streams into manageable chunks for algorithmic processing.
Implements deterministic chunking strategies without external dependencies.
"""


class Chunking:
    """Deterministic chunking strategies for tick stream processing."""

    @staticmethod
    def chunk_by_size(ticks: list[dict], chunk_size: int) -> list[list[dict]]:
        """
        Split tick stream into fixed-size chunks.

        Args:
            ticks: List of tick dictionaries with market data
            chunk_size: Number of ticks per chunk (must be positive integer)

        Returns:
            List of chunks, each containing up to chunk_size ticks.
            Last chunk may contain fewer ticks.

        Raises:
            ValueError: If chunk_size is not a positive integer

        Example:
            >>> ticks = [{'t': 1, 'p': 100}, {'t': 2, 'p': 101}]
            >>> Chunking.chunk_by_size(ticks, 1)
            [[{'t': 1, 'p': 100}], [{'t': 2, 'p': 101}]]
        """
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError("chunk_size must be a positive integer")

        chunks = []
        for i in range(0, len(ticks), chunk_size):
            chunks.append(ticks[i : i + chunk_size])
        return chunks

    @staticmethod
    def chunk_by_time(ticks: list[dict], time_window_us: int) -> list[list[dict]]:
        """
        Split tick stream into chunks based on time windows.

        Groups consecutive ticks where time delta doesn't exceed time_window_us.
        Each chunk starts a new window when the time gap exceeds the threshold.

        Args:
            ticks: List of tick dictionaries with 't' field (timestamp in microseconds)
            time_window_us: Maximum time span per chunk in microseconds (positive integer)

        Returns:
            List of chunks grouped by time windows.
            Empty input returns empty list.

        Raises:
            ValueError: If time_window_us is not a positive integer
            KeyError: If any tick lacks 't' field

        Example:
            >>> ticks = [{'t': 0, 'p': 100}, {'t': 500, 'p': 101}]
            >>> Chunking.chunk_by_time(ticks, 1000)
            [[{'t': 0, 'p': 100}, {'t': 500, 'p': 101}]]
        """
        if not isinstance(time_window_us, int) or time_window_us <= 0:
            raise ValueError("time_window_us must be a positive integer")

        if not ticks:
            return []

        chunks = []
        current_chunk = []
        window_start_time = None

        for tick in ticks:
            if "t" not in tick:
                raise KeyError(f"Tick missing 't' field: {tick}")

            tick_time = tick["t"]

            # Validate tick_time is integer
            if not isinstance(tick_time, int):
                raise ValueError(
                    f"Tick timestamp must be integer, got {type(tick_time).__name__}"
                )

            # Initialize window on first tick
            if window_start_time is None:
                window_start_time = tick_time
                current_chunk.append(tick)
            else:
                # Check if tick exceeds time window
                time_delta = tick_time - window_start_time
                if time_delta <= time_window_us:
                    current_chunk.append(tick)
                else:
                    # Start new chunk
                    chunks.append(current_chunk)
                    current_chunk = [tick]
                    window_start_time = tick_time

        # Append final chunk if non-empty
        if current_chunk:
            chunks.append(current_chunk)

        return chunks
