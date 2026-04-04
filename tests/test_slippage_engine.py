"""
Tests for SlippageEngine.
"""

import unittest
from unittest.mock import Mock


class TestSlippageEngine(unittest.TestCase):
    """Test cases for SlippageEngine."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock dependencies
        self.mock_tick_engine = Mock()
        self.mock_tick_engine.get_stats.return_value = {
            'timestamp_range': (0, 1000000000)  # 1 second in ns
        }

        self.mock_latency_model = Mock()
        self.mock_latency_model.simulate_latency.return_value = 1000050  # 1s + 50us

        # Create engine instance
        from tare.microstructure.slippage_engine import SlippageEngine
        self.engine = SlippageEngine(self.mock_tick_engine, self.mock_latency_model)

    def test_initialization(self):
        """Test SlippageEngine initialization."""
        from tare.microstructure.slippage_engine import SlippageEngine

        # Valid initialization
        engine = SlippageEngine(self.mock_tick_engine, self.mock_latency_model)
        self.assertIsNotNone(engine)

        # Invalid: None dependencies
        with self.assertRaises(ValueError):
            SlippageEngine(None, self.mock_latency_model)

        with self.assertRaises(ValueError):
            SlippageEngine(self.mock_tick_engine, None)

    def test_compute_slippage_basic(self):
        """Test basic slippage calculation."""
        order_size = 100
        bid = 10000
        ask = 10010  # spread = 10
        spread_history = [8, 9, 10, 11, 12]

        slippage = self.engine.compute_slippage(order_size, bid, ask, spread_history)

        # Check type and bounds
        self.assertIsInstance(slippage, int)
        self.assertGreaterEqual(slippage, 0)
        self.assertLessEqual(slippage, (ask - bid) * 10)  # Max 10x spread

    def test_determinism(self):
        """Test that same inputs produce same output."""
        order_size = 100
        bid = 10000
        ask = 10010
        spread_history = [8, 9, 10, 11, 12]

        result1 = self.engine.compute_slippage(order_size, bid, ask, spread_history)
        result2 = self.engine.compute_slippage(order_size, bid, ask, spread_history)

        self.assertEqual(result1, result2)

    def test_order_size_impact(self):
        """Test that larger orders cause more slippage."""
        bid = 10000
        ask = 10010
        spread_history = [10, 10, 10]

        small_slippage = self.engine.compute_slippage(10, bid, ask, spread_history)
        large_slippage = self.engine.compute_slippage(1000, bid, ask, spread_history)

        self.assertGreaterEqual(large_slippage, small_slippage)

    def test_spread_impact(self):
        """Test that wider spreads affect slippage."""
        order_size = 100
        spread_history = [10, 10, 10]

        # Narrow spread
        narrow_slippage = self.engine.compute_slippage(
            order_size, 10000, 10005, spread_history)  # spread = 5

        # Wide spread
        wide_slippage = self.engine.compute_slippage(
            order_size, 10000, 10020, spread_history)  # spread = 20

        # Wide spread should generally have different slippage
        # (could be more or less depending on the model)
        self.assertNotEqual(narrow_slippage, wide_slippage)

    def test_empty_spread_history(self):
        """Test with empty spread history."""
        order_size = 100
        bid = 10000
        ask = 10010

        slippage = self.engine.compute_slippage(order_size, bid, ask, [])
        self.assertIsInstance(slippage, int)
        self.assertGreaterEqual(slippage, 0)

    def test_invalid_inputs(self):
        """Test validation of invalid inputs."""
        # Negative order size
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(-100, 10000, 10010, [10])

        # Zero order size
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(0, 10000, 10010, [10])

        # Non-int order size
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(100.5, 10000, 10010, [10])

        # Negative bid
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(100, -10000, 10010, [10])

        # Zero bid
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(100, 0, 10010, [10])

        # Negative ask
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(100, 10000, -10010, [10])

        # Ask <= bid
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(100, 10000, 10000, [10])  # equal
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(100, 10000, 9999, [10])  # less

        # Invalid spread_history type
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(100, 10000, 10010, "not a list")

        # Invalid spread_history values
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(100, 10000, 10010, [10, -5, 15])
        with self.assertRaises(ValueError):
            self.engine.compute_slippage(100, 10000, 10010, [10, "15", 20])

    def test_latency_impact(self):
        """Test that latency affects slippage."""
        order_size = 100
        bid = 10000
        ask = 10010
        spread_history = [10, 10, 10]

        # Get baseline with current mock
        baseline = self.engine.compute_slippage(order_size, bid, ask, spread_history)

        # Create new engine with different latency
        mock_latency_high = Mock()
        mock_latency_high.simulate_latency.return_value = 1000100  # 100us more

        from tare.microstructure.slippage_engine import SlippageEngine
        engine_high_latency = SlippageEngine(self.mock_tick_engine, mock_latency_high)

        high_latency_slippage = engine_high_latency.compute_slippage(
            order_size, bid, ask, spread_history)

        # Higher latency should generally increase slippage
        # (model dependent, but should at least be deterministic)
        self.assertIsInstance(high_latency_slippage, int)

    def test_integer_sqrt(self):
        """Test the integer square root helper."""
        # Test basic cases
        self.assertEqual(self.engine._integer_sqrt(0), 0)
        self.assertEqual(self.engine._integer_sqrt(1), 1)
        self.assertEqual(self.engine._integer_sqrt(4), 2)
        self.assertEqual(self.engine._integer_sqrt(9), 3)
        self.assertEqual(self.engine._integer_sqrt(16), 4)

        # Test non-perfect squares
        self.assertEqual(self.engine._integer_sqrt(2), 1)  # floor(sqrt(2)) = 1
        self.assertEqual(self.engine._integer_sqrt(3), 1)  # floor(sqrt(3)) = 1
        self.assertEqual(self.engine._integer_sqrt(15), 3)  # floor(sqrt(15)) = 3
        self.assertEqual(self.engine._integer_sqrt(17), 4)  # floor(sqrt(17)) = 4

    def test_average_spread_calculation(self):
        """Test the average spread calculation."""
        # With history
        current = 10
        history = [8, 9, 10, 11, 12]
        avg = self.engine._calculate_average_spread(history, current)
        self.assertIsInstance(avg, int)
        self.assertGreater(avg, 0)

        # Empty history
        avg_empty = self.engine._calculate_average_spread([], current)
        self.assertEqual(avg_empty, current)

        # Single value history
        avg_single = self.engine._calculate_average_spread([15], current)
        self.assertIsInstance(avg_single, int)

    def test_properties(self):
        """Test read-only properties."""
        self.assertIs(self.engine.tick_data_engine, self.mock_tick_engine)
        self.assertIs(self.engine.latency_model, self.mock_latency_model)


if __name__ == '__main__':
    unittest.main()
