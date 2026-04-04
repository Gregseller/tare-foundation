"""
TARE tick_data_engine module - Orchestrate tick ingestion, cleaning, correction pipeline.
Phase 1: Core tick data orchestration with deterministic processing.
"""

import csv
from pathlib import Path
from typing import Generator, Optional

from tare.tick_data_engine.tick_cleaner import TickCleaner
from tare.tick_data_engine.jitter_corrector import JitterCorrector


class TickDataEngine:
    """
    Orchestrate tick data ingestion, cleaning, and correction pipeline.
    
    Manages:
    - Tick ingestion from various sources (CSV, binary)
    - Data cleaning (deduplication, validation, standardization)
    - Jitter correction and timestamp synchronization
    - Storage and export of processed tick snapshots
    """

    def __init__(self, max_jitter_us: int = 1000):
        """
        Initialize TickDataEngine.

        Args:
            max_jitter_us: Maximum acceptable jitter in microseconds (int).
                          Default 1000 microseconds (1 millisecond).

        Raises:
            ValueError: If max_jitter_us is not a positive int.
        """
        if not isinstance(max_jitter_us, int) or max_jitter_us <= 0:
            raise ValueError("max_jitter_us must be a positive int")

        self._cleaner = TickCleaner()
        self._corrector = JitterCorrector(max_jitter_us=max_jitter_us)
        self._clean_ticks = []
        self._raw_ticks = []

    def ingest(self, source: str, format: str) -> None:
        """
        Ingest tick data from source file and apply cleaning pipeline.

        Supported formats:
        - 'csv': Comma-separated values with headers (timestamp, symbol, price, volume)
        - 'binary': Binary fixed-format records (28 bytes per tick)

        Pipeline:
        1. Read ticks from source file
        2. Filter invalid ticks
        3. Remove duplicates
        4. Standardize data
        5. Store cleaned ticks in memory

        Args:
            source: File system path to source file (str).
            format: Data format type ('csv' or 'binary').

        Raises:
            FileNotFoundError: If source file does not exist.
            ValueError: If format is unsupported or file is invalid.
        """
        if not isinstance(source, str):
            raise ValueError("source must be a string path")

        if not isinstance(format, str):
            raise ValueError("format must be a string")

        if format not in ('csv', 'binary'):
            raise ValueError(f"Unsupported format: {format}. Must be 'csv' or 'binary'")

        # Read ticks based on format
        if format == 'csv':
            ticks = self._read_csv_ticks(source)
        else:  # binary
            ticks = self._read_binary_ticks(source)

        # Store raw ticks for reference
        self._raw_ticks = ticks

        # Apply cleaning pipeline
        cleaned = self._cleaner.clean(ticks)

        self._clean_ticks = cleaned

    def get_clean_ticks(self) -> list[dict]:
        """
        Retrieve cleaned and processed tick data.

        Returns:
            List of cleaned tick dictionaries with fields:
            - timestamp (int): Nanoseconds since epoch
            - symbol (str): Asset symbol
            - price (int): Price in base units
            - volume (int): Volume in units

        Raises:
            ValueError: If no data has been ingested yet.
        """
        if len(self._clean_ticks) == 0:
            raise ValueError("No clean ticks available. Call ingest() first.")

        return self._clean_ticks.copy()

    def correct_jitter(self, max_jitter_us: int = None) -> None:
        """
        Apply jitter correction to current clean ticks.

        Detects and corrects timing artifacts within tolerance threshold.
        Updates internal clean ticks with corrected timestamps.

        Args:
            max_jitter_us: Override instance max_jitter_us for this correction (int).
                          If None, uses instance value.

        Raises:
            ValueError: If max_jitter_us is invalid or no data ingested.
        """
        if len(self._clean_ticks) == 0:
            raise ValueError("No clean ticks available. Call ingest() first.")

        if max_jitter_us is None:
            max_jitter_us = self._corrector.max_jitter_us
        if not isinstance(max_jitter_us, int) or max_jitter_us <= 0:
            raise ValueError("max_jitter_us must be a positive int")

        corrected = self._corrector.correct_timestamps(
            self._clean_ticks,
            max_jitter_us=max_jitter_us
        )

        self._clean_ticks = corrected

    def synchronize_sources(self, sources: dict[str, str], format: str) -> None:
        """
        Ingest and synchronize tick data from multiple sources.

        Ingests ticks from multiple files, cleans each independently,
        then synchronizes and corrects jitter across all sources.

        Pipeline:
        1. Ingest and clean ticks from each source file
        2. Correct jitter in each source
        3. Synchronize all sources by timestamp
        4. Store merged result in clean_ticks

        Args:
            sources: Dict mapping source name (str) to file path (str).
            format: Data format type ('csv' or 'binary').

        Raises:
            FileNotFoundError: If any source file does not exist.
            ValueError: If format is unsupported or files are invalid.
        """
        if not isinstance(sources, dict):
            raise ValueError("sources must be a dict")

        if not isinstance(format, str):
            raise ValueError("format must be a string")

        if format not in ('csv', 'binary'):
            raise ValueError(f"Unsupported format: {format}. Must be 'csv' or 'binary'")

        # Ingest and clean ticks from each source
        ticks_by_source = {}

        for source_name, file_path in sources.items():
            if not isinstance(source_name, str):
                raise ValueError("Source names must be strings")
            if not isinstance(file_path, str):
                raise ValueError("File paths must be strings")

            # Read ticks from source
            if format == 'csv':
                ticks = self._read_csv_ticks(file_path)
            else:  # binary
                ticks = self._read_binary_ticks(file_path)

            # Clean ticks
            cleaned = self._cleaner.clean(ticks)

            ticks_by_source[source_name] = cleaned

        # Synchronize and correct jitter across sources
        synchronized = self._corrector.synchronize_sources(ticks_by_source)

        self._clean_ticks = synchronized

    def export_snapshot(self, path: str) -> None:
        """
        Export current clean ticks to CSV file.

        Creates CSV file with headers: timestamp, symbol, price, volume
        One tick per row with deterministic ordering.

        Args:
            path: File system path for output CSV file (str).

        Raises:
            ValueError: If no clean data available or path is invalid.
            IOError: If file write fails.
        """
        if not isinstance(path, str):
            raise ValueError("path must be a string")

        if len(self._clean_ticks) == 0:
            raise ValueError("No clean ticks available to export. Call ingest() first.")

        try:
            file_path = Path(path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['timestamp', 'symbol', 'price', 'volume']
                writer = csv.DictWriter(f, fieldnames=fieldnames)

                writer.writeheader()

                for tick in self._clean_ticks:
                    writer.writerow({
                        'timestamp': tick['timestamp'],
                        'symbol': tick['symbol'],
                        'price': tick['price'],
                        'volume': tick['volume']
                    })

        except IOError as e:
            raise IOError(f"Failed to write CSV to {path}: {e}")

    def stream_clean_ticks(self) -> Generator[dict, None, None]:
        """
        Stream clean ticks one at a time without loading all into memory.

        Yields:
            Individual cleaned tick dictionaries.

        Raises:
            ValueError: If no clean data available.
        """
        if len(self._clean_ticks) == 0:
            raise ValueError("No clean ticks available. Call ingest() first.")

        for tick in self._clean_ticks:
            yield tick

    def get_stats(self) -> dict:
        """
        Get statistics about ingested and cleaned tick data.

        Returns:
            Dict with keys:
            - raw_count (int): Total ticks ingested
            - clean_count (int): Ticks after cleaning
            - unique_symbols (int): Count of unique symbols in clean data
            - timestamp_range (tuple): (min_ts, max_ts) of clean ticks

        Raises:
            ValueError: If no data has been ingested.
        """
        if len(self._raw_ticks) == 0:
            raise ValueError("No data available. Call ingest() first.")

        symbols = set()
        min_ts = None
        max_ts = None

        for tick in self._clean_ticks:
            symbols.add(tick['symbol'])
            ts = tick['timestamp']

            if min_ts is None or ts < min_ts:
                min_ts = ts
            if max_ts is None or ts > max_ts:
                max_ts = ts

        return {
            'raw_count': len(self._raw_ticks),
            'clean_count': len(self._clean_ticks),
            'unique_symbols': len(symbols),
            'timestamp_range': (min_ts if min_ts is not None else 0,
                              max_ts if max_ts is not None else 0)
        }

    @staticmethod
    def _read_csv_ticks(path: str) -> list[dict]:
        """
        Read and parse CSV tick file.

        Args:
            path: File system path to CSV file.

        Returns:
            List of tick dictionaries.

        Raises:
            FileNotFoundError: If file does not exist.
            ValueError: If CSV format is invalid.
        """
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        ticks = []

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            if reader.fieldnames is None:
                raise ValueError("CSV file is empty or has no header")

            required_fields = {'timestamp', 'symbol', 'price', 'volume'}
            if not required_fields.issubset(set(reader.fieldnames)):
                raise ValueError(
                    f"CSV missing required fields. Expected {required_fields}, "
                    f"got {set(reader.fieldnames)}"
                )

            for row_num, row in enumerate(reader, start=2):
                try:
                    timestamp = int(row['timestamp'])
                    symbol = row['symbol'].strip()
                    price = int(row['price'])
                    volume = int(row['volume'])

                    ticks.append({
                        'timestamp': timestamp,
                        'symbol': symbol,
                        'price': price,
                        'volume': volume
                    })

                except ValueError as e:
                    raise ValueError(f"Row {row_num}: Failed to parse values as integers: {e}")
                except KeyError as e:
                    raise ValueError(f"Row {row_num}: Missing field {e}")

        return ticks

    @staticmethod
    def _read_binary_ticks(path: str) -> list[dict]:
        """
        Read and parse binary tick file.

        Format: timestamp(8) + symbol(4) + price(8) + volume(8) = 28 bytes per tick

        Args:
            path: File system path to binary file.

        Returns:
            List of tick dictionaries.

        Raises:
            FileNotFoundError: If file does not exist.
            ValueError: If binary format is invalid.
        """
        import struct

        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        BINARY_TICK_SIZE = 28
        file_size = file_path.stat().st_size

        if file_size % BINARY_TICK_SIZE != 0:
            raise ValueError(
                f"Binary file size {file_size} is not multiple of {BINARY_TICK_SIZE}"
            )

        ticks = []

        with open(file_path, 'rb') as f:
            while True:
                data = f.read(BINARY_TICK_SIZE)
                if not data:
                    break

                try:
                    timestamp, s1, s2, s3, s4, price, volume = struct.unpack('>QBBBBqq', data)

                    symbol_bytes = bytes([s1, s2, s3, s4])
                    symbol = symbol_bytes.rstrip(b'\x00').decode('ascii')

                    if not symbol:
                        symbol = 'UNKNOWN'

                    ticks.append({
                        'timestamp': timestamp,
                        'symbol': symbol,
                        'price': price,
                        'volume': volume
                    })

                except struct.error as e:
                    raise ValueError(f"Failed to unpack binary tick data: {e}")
                except UnicodeDecodeError as e:
                    raise ValueError(f"Failed to decode symbol from binary data: {e}")

        return ticks
