"""
Unit tests for the TickCleaner module.
"""

import unittest
from tare.tick_data_engine.tick_cleaner import TickCleaner


class TestTickCleaner(unittest.TestCase):
    """Test cases for TickCleaner class."""

    def setUp(self):
        """Set up test data."""
        self.valid_ticks = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
            {'timestamp': 1002, 'symbol': 'MSFT', 'price': 30000, 'volume': 200},
        ]

    def test_remove_duplicates_basic(self):
        """Test removing duplicate ticks."""
        ticks_with_duplicates = self.valid_ticks + [self.valid_ticks[0]]
        result = TickCleaner.remove_duplicates(ticks_with_duplicates)
        self.assertEqual(result, self.valid_ticks)

    def test_remove_duplicates_multiple_duplicates(self):
        """Test removing multiple duplicate ticks."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
        ]
        expected = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
        ]
        result = TickCleaner.remove_duplicates(ticks)
        self.assertEqual(result, expected)

    def test_remove_duplicates_preserves_order(self):
        """Test that order is preserved when removing duplicates."""
        ticks = [
            {'timestamp': 1002, 'symbol': 'MSFT', 'price': 30000, 'volume': 200},
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
        ]
        expected = [
            {'timestamp': 1002, 'symbol': 'MSFT', 'price': 30000, 'volume': 200},
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
        ]
        result = TickCleaner.remove_duplicates(ticks)
        self.assertEqual(result, expected)

    def test_remove_duplicates_missing_fields(self):
        """Test that ticks missing required fields raise error."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000},  # Missing volume
        ]
        with self.assertRaises(ValueError):
            TickCleaner.remove_duplicates(ticks)

    def test_remove_duplicates_invalid_input(self):
        """Test that non-list input raises error."""
        with self.assertRaises(ValueError):
            TickCleaner.remove_duplicates("not a list")
        with self.assertRaises(ValueError):
            TickCleaner.remove_duplicates([1, 2, 3])  # List of non-dicts

    def test_filter_invalid_basic(self):
        """Test filtering invalid ticks."""
        ticks = self.valid_ticks + [
            {'timestamp': -1, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},  # Negative timestamp
            {'timestamp': 1003, 'symbol': '', 'price': 15000, 'volume': 100},  # Empty symbol
            {'timestamp': 1004, 'symbol': 'AAPL', 'price': -100, 'volume': 100},  # Negative price
            {'timestamp': 1005, 'symbol': 'AAPL', 'price': 15000, 'volume': 0},  # Zero volume
            {'timestamp': 1006, 'symbol': 'AAPL', 'price': 15000, 'volume': -10},  # Negative volume
            {'timestamp': 1007, 'symbol': 'AAPL', 'price': 'invalid', 'volume': 100},  # Non-int price
            {'timestamp': 'invalid', 'symbol': 'AAPL', 'price': 15000, 'volume': 100},  # Non-int timestamp
        ]
        result = TickCleaner.filter_invalid(ticks)
        self.assertEqual(result, self.valid_ticks)

    def test_filter_invalid_missing_keys(self):
        """Test filtering ticks with missing keys."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000},  # Missing volume
            {'timestamp': 1001, 'symbol': 'AAPL', 'volume': 100},  # Missing price
            {'timestamp': 1002, 'price': 15000, 'volume': 100},  # Missing symbol
        ]
        result = TickCleaner.filter_invalid(ticks)
        self.assertEqual(result, [])

    def test_filter_invalid_preserves_order(self):
        """Test that order is preserved when filtering."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},  # Valid
            {'timestamp': -1, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},  # Invalid
            {'timestamp': 1002, 'symbol': 'MSFT', 'price': 30000, 'volume': 200},  # Valid
        ]
        expected = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1002, 'symbol': 'MSFT', 'price': 30000, 'volume': 200},
        ]
        result = TickCleaner.filter_invalid(ticks)
        self.assertEqual(result, expected)

    def test_filter_invalid_invalid_input(self):
        """Test that non-list input raises error."""
        with self.assertRaises(ValueError):
            TickCleaner.filter_invalid("not a list")
        with self.assertRaises(ValueError):
            TickCleaner.filter_invalid([1, 2, 3])  # List of non-dicts

    def test_standardize_basic(self):
        """Test standardizing tick data."""
        ticks = [
            {'timestamp': 1000, 'symbol': ' aapl ', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GoOgL', 'price': 280000, 'volume': 50},
            {'timestamp': 1002, 'symbol': 'msft', 'price': 30000, 'volume': 200},
        ]
        expected = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
            {'timestamp': 1002, 'symbol': 'MSFT', 'price': 30000, 'volume': 200},
        ]
        result = TickCleaner.standardize(ticks)
        self.assertEqual(result, expected)

    def test_standardize_preserves_order(self):
        """Test that order is preserved when standardizing."""
        result = TickCleaner.standardize(self.valid_ticks)
        self.assertEqual(result, self.valid_ticks)  # Already standardized

    def test_standardize_invalid_input(self):
        """Test that non-list input raises error."""
        with self.assertRaises(ValueError):
            TickCleaner.standardize("not a list")
        with self.assertRaises(ValueError):
            TickCleaner.standardize([1, 2, 3])  # List of non-dicts

    def test_clean_full_pipeline(self):
        """Test the full cleaning pipeline."""
        ticks = [
            {'timestamp': 1000, 'symbol': ' aapl ', 'price': 15000, 'volume': 100},  # Valid, needs standardization
            {'timestamp': 1000, 'symbol': ' aapl ', 'price': 15000, 'volume': 100},  # Duplicate
            {'timestamp': -1, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},  # Invalid timestamp
            {'timestamp': 1001, 'symbol': 'GoOgL', 'price': 280000, 'volume': 50},  # Valid, needs standardization
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},  # Duplicate after standardization
            {'timestamp': 1002, 'symbol': '', 'price': 30000, 'volume': 200},  # Empty symbol
            {'timestamp': 1003, 'symbol': 'MSFT', 'price': -100, 'volume': 200},  # Negative price
        ]
        expected = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
        ]
        result = TickCleaner.clean(ticks)
        self.assertEqual(result, expected)

    def test_clean_empty_list(self):
        """Test cleaning an empty list."""
        result = TickCleaner.clean([])
        self.assertEqual(result, [])

    def test_clean_all_invalid(self):
        """Test cleaning a list with all invalid ticks."""
        ticks = [
            {'timestamp': -1, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1000, 'symbol': '', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'AAPL', 'price': -100, 'volume': 100},
        ]
        result = TickCleaner.clean(ticks)
        self.assertEqual(result, [])

    def test_clean_generator_basic(self):
        """Test cleaning with generator input."""
        def tick_generator():
            yield {'timestamp': 1000, 'symbol': ' aapl ', 'price': 15000, 'volume': 100}
            yield {'timestamp': 1000, 'symbol': ' aapl ', 'price': 15000, 'volume': 100}  # Duplicate
            yield {'timestamp': -1, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50}  # Invalid
            yield {'timestamp': 1001, 'symbol': 'GoOgL', 'price': 280000, 'volume': 50}

        result = list(TickCleaner.clean_generator(tick_generator()))
        expected = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
        ]
        self.assertEqual(result, expected)

    def test_clean_generator_empty(self):
        """Test cleaning with empty generator."""
        def empty_generator():
            return
            yield

        result = list(TickCleaner.clean_generator(empty_generator()))
        self.assertEqual(result, [])

    def test_determinism(self):
        """Test that cleaning is deterministic (same input → same output)."""
        ticks = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1001, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},  # Duplicate
            {'timestamp': 1002, 'symbol': 'msft', 'price': 30000, 'volume': 200},
        ]

        # Run multiple times
        result1 = TickCleaner.clean(ticks)
        result2 = TickCleaner.clean(ticks)
        result3 = TickCleaner.clean(ticks)

        self.assertEqual(result1, result2)
        self.assertEqual(result2, result3)

    def test_no_float_values(self):
        """Test that methods don't introduce float values."""
        # This test ensures we're following TARE rule 1: ONLY int
        ticks = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
        ]

        result = TickCleaner.clean(ticks)
        
        # Check all values are integers
        for tick in result:
            self.assertIsInstance(tick['timestamp'], int)
            self.assertIsInstance(tick['price'], int)
            self.assertIsInstance(tick['volume'], int)
            self.assertIsInstance(tick['symbol'], str)


if __name__ == '__main__':
    unittest.main()
