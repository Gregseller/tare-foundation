"""
TARE tick_cleaner module - Remove duplicates, filter invalid ticks, standardize data.
Phase 1: Core tick data cleaning with deterministic processing.
"""

from typing import Generator, Optional


class TickCleaner:
    """Clean tick data by removing duplicates, filtering invalid ticks, and standardizing."""

    @staticmethod
    def remove_duplicates(ticks: list[dict]) -> list[dict]:
        """
        Remove duplicate ticks based on timestamp, symbol, price, and volume.
        For ticks with identical keys, the first occurrence is kept.

        Args:
            ticks: List of tick dictionaries.

        Returns:
            List of unique tick dictionaries, preserving original order.

        Raises:
            ValueError: If input is not a list or contains non-dict items.
        """
        if not isinstance(ticks, list):
            raise ValueError("Input must be a list of ticks")

        seen_keys = set()
        unique_ticks = []

        for tick in ticks:
            if not isinstance(tick, dict):
                raise ValueError("All tick items must be dictionaries")

            # Create a hashable key from the tick's core data
            key = (
                tick.get('timestamp'),
                tick.get('symbol'),
                tick.get('price'),
                tick.get('volume')
            )

            # Ensure all required fields are present for deduplication
            if None in key:
                raise ValueError("Tick missing required fields for deduplication")

            if key not in seen_keys:
                seen_keys.add(key)
                unique_ticks.append(tick)

        return unique_ticks

    @staticmethod
    def filter_invalid(ticks: list[dict]) -> list[dict]:
        """
        Filter out ticks with invalid values based on TARE rules.

        Invalid ticks are those with:
        - Non-integer timestamp, price, or volume
        - Negative timestamp or price
        - Non-positive volume (<= 0)
        - Empty or non-string symbol

        Args:
            ticks: List of tick dictionaries.

        Returns:
            List of valid tick dictionaries, preserving order of valid ticks.

        Raises:
            ValueError: If input is not a list or contains non-dict items.
        """
        if not isinstance(ticks, list):
            raise ValueError("Input must be a list of ticks")

        valid_ticks = []

        for tick in ticks:
            if not isinstance(tick, dict):
                raise ValueError("All tick items must be dictionaries")

            # Check for required keys
            required_keys = {'timestamp', 'symbol', 'price', 'volume'}
            if not required_keys.issubset(tick.keys()):
                continue  # Skip ticks missing required fields

            # Validate timestamp
            if not isinstance(tick['timestamp'], int):
                continue
            if tick['timestamp'] < 0:
                continue

            # Validate symbol
            if not isinstance(tick['symbol'], str):
                continue
            if len(tick['symbol']) == 0:
                continue

            # Validate price
            if not isinstance(tick['price'], int):
                continue
            if tick['price'] < 0:
                continue

            # Validate volume
            if not isinstance(tick['volume'], int):
                continue
            if tick['volume'] <= 0:
                continue

            valid_ticks.append(tick)

        return valid_ticks

    @staticmethod
    def standardize(ticks: list[dict]) -> list[dict]:
        """
        Standardize tick data to ensure consistent format.

        Ensures:
        - Symbol is uppercase and stripped of whitespace
        - All values are integers (already enforced by filter_invalid)
        - Dictionary keys are in consistent order

        Args:
            ticks: List of tick dictionaries.

        Returns:
            List of standardized tick dictionaries.

        Raises:
            ValueError: If input is not a list or contains non-dict items.
        """
        if not isinstance(ticks, list):
            raise ValueError("Input must be a list of ticks")

        standardized_ticks = []

        for tick in ticks:
            if not isinstance(tick, dict):
                raise ValueError("All tick items must be dictionaries")

            # Create a new tick with standardized format
            standardized_tick = {
                'timestamp': tick['timestamp'],
                'symbol': tick['symbol'].strip().upper(),
                'price': tick['price'],
                'volume': tick['volume']
            }

            standardized_ticks.append(standardized_tick)

        return standardized_ticks

    @staticmethod
    def clean(ticks: list[dict]) -> list[dict]:
        """
        Apply full cleaning pipeline: filter, standardize, then deduplicate.

        This is the recommended method for most use cases as it ensures
        all ticks are valid before deduplication.

        Args:
            ticks: List of tick dictionaries.

        Returns:
            Cleaned list of tick dictionaries.
        """
        valid_ticks = TickCleaner.filter_invalid(ticks)
        standardized_ticks = TickCleaner.standardize(valid_ticks)
        unique_ticks = TickCleaner.remove_duplicates(standardized_ticks)
        return unique_ticks

    @staticmethod
    def clean_generator(ticks: Generator[dict, None, None]) -> Generator[dict, None, None]:
        """
        Apply cleaning pipeline to a generator of ticks.

        This method processes ticks one by one to avoid loading all data
        into memory, but note that deduplication requires tracking seen keys.

        Args:
            ticks: Generator yielding tick dictionaries.

        Yields:
            Cleaned tick dictionaries.
        """
        seen_keys = set()

        for tick in ticks:
            # Filter invalid ticks
            if not isinstance(tick, dict):
                continue

            required_keys = {'timestamp', 'symbol', 'price', 'volume'}
            if not required_keys.issubset(tick.keys()):
                continue

            if not isinstance(tick['timestamp'], int) or tick['timestamp'] < 0:
                continue
            if not isinstance(tick['symbol'], str) or len(tick['symbol']) == 0:
                continue
            if not isinstance(tick['price'], int) or tick['price'] < 0:
                continue
            if not isinstance(tick['volume'], int) or tick['volume'] <= 0:
                continue

            # Standardize
            standardized_tick = {
                'timestamp': tick['timestamp'],
                'symbol': tick['symbol'].strip().upper(),
                'price': tick['price'],
                'volume': tick['volume']
            }

            # Deduplicate
            key = (
                standardized_tick['timestamp'],
                standardized_tick['symbol'],
                standardized_tick['price'],
                standardized_tick['volume']
            )

            if key not in seen_keys:
                seen_keys.add(key)
                yield standardized_tick
