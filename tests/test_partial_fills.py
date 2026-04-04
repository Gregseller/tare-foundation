"""
Unit tests for partial_fills module.

Tests PartialFillSimulator class with various market depth scenarios.
All tests are deterministic and follow TARE rules.
"""

import unittest
from tare.microstructure.partial_fills import PartialFillSimulator


class TestPartialFillSimulator(unittest.TestCase):
    """Test cases for PartialFillSimulator."""

    def setUp(self):
        """Set up test fixture."""
        self.simulator = PartialFillSimulator()

    def test_initialization(self):
        """Test simulator initialization."""
        simulator = PartialFillSimulator()
        self.assertIsInstance(simulator, PartialFillSimulator)

    def test_fill_order_zero_size(self):
        """Test filling order with zero size."""
        market_depth = {
            'bids': [(100, 100)],
            'asks': [(101, 100)]
        }
        result = self.simulator.fill_order(0, market_depth)
        self.assertEqual(result, [])

    def test_fill_order_negative_size(self):
        """Test filling order with negative size (sell order)."""
        market_depth = {
            'bids': [(100, 50), (99, 100)],
            'asks': [(101, 100)]
        }
        result = self.simulator.fill_order(-30, market_depth)
        # Should fill 30 at best bid (100)
        self.assertEqual(result, [(100, -30)])

    def test_fill_buy_order_single_level(self):
        """Test filling buy order with single ask level."""
        market_depth = {
            'bids': [(100, 100)],
            'asks': [(101, 50)]
        }
        result = self.simulator.fill_order(30, market_depth)
        self.assertEqual(result, [(101, 30)])

    def test_fill_buy_order_multiple_levels(self):
        """Test filling buy order across multiple ask levels."""
        market_depth = {
            'bids': [(100, 100)],
            'asks': [(101, 20), (102, 30), (103, 50)]
        }
        result = self.simulator.fill_order(80, market_depth)
        expected = [(101, 20), (102, 30), (103, 30)]
        self.assertEqual(result, expected)

    def test_fill_sell_order_multiple_levels(self):
        """Test filling sell order across multiple bid levels."""
        market_depth = {
            'bids': [(100, 40), (99, 30), (98, 50)],
            'asks': [(101, 100)]
        }
        result = self.simulator.fill_order(-100, market_depth)
        expected = [(100, -40), (99, -30), (98, -30)]
        self.assertEqual(result, expected)

    def test_fill_order_insufficient_liquidity(self):
        """Test filling order when market depth is insufficient."""
        market_depth = {
            'bids': [(100, 10), (99, 10)],
            'asks': [(101, 10), (102, 10)]
        }
        result = self.simulator.fill_order(50, market_depth)
        # Should fill all available asks (10 + 10 = 20)
        expected = [(101, 10), (102, 10)]
        self.assertEqual(result, expected)

    def test_fill_order_exact_liquidity(self):
        """Test filling order with exact available liquidity."""
        market_depth = {
            'bids': [(100, 25), (99, 25)],
            'asks': [(101, 25), (102, 25)]
        }
        result = self.simulator.fill_order(50, market_depth)
        expected = [(101, 25), (102, 25)]
        self.assertEqual(result, expected)

    def test_fill_order_empty_market_depth(self):
        """Test filling order with empty market depth."""
        market_depth = {
            'bids': [],
            'asks': []
        }
        result = self.simulator.fill_order(100, market_depth)
        self.assertEqual(result, [])

    def test_fill_order_zero_volume_levels(self):
        """Test filling order with zero volume levels in market depth."""
        market_depth = {
            'bids': [(100, 0), (99, 50)],
            'asks': [(101, 0), (102, 0), (103, 60)]
        }
        result = self.simulator.fill_order(40, market_depth)
        # Should skip zero volume levels
        self.assertEqual(result, [(103, 40)])

    def test_invalid_order_size_type(self):
        """Test with non-integer order size."""
        market_depth = {
            'bids': [(100, 100)],
            'asks': [(101, 100)]
        }
        with self.assertRaises(ValueError):
            self.simulator.fill_order(100.5, market_depth)

    def test_invalid_market_depth_type(self):
        """Test with non-dictionary market depth."""
        with self.assertRaises(ValueError):
            self.simulator.fill_order(100, "not a dict")

    def test_missing_market_depth_keys(self):
        """Test with market depth missing required keys."""
        with self.assertRaises(ValueError):
            self.simulator.fill_order(100, {'bids': []})

        with self.assertRaises(ValueError):
            self.simulator.fill_order(100, {'asks': []})

    def test_invalid_price_type(self):
        """Test with non-integer price in market depth."""
        market_depth = {
            'bids': [(100.5, 100)],
            'asks': [(101, 100)]
        }
        with self.assertRaises(ValueError):
            self.simulator.fill_order(100, market_depth)

    def test_invalid_volume_type(self):
        """Test with non-integer volume in market depth."""
        market_depth = {
            'bids': [(100, 100)],
            'asks': [(101, 100.5)]
        }
        with self.assertRaises(ValueError):
            self.simulator.fill_order(100, market_depth)

    def test_calculate_execution_price_empty(self):
        """Test calculating execution price with empty fills."""
        result = self.simulator.calculate_execution_price([])
        self.assertEqual(result, 0)

    def test_calculate_execution_price_single_fill(self):
        """Test calculating execution price with single fill."""
        fills = [(100, 50)]
        result = self.simulator.calculate_execution_price(fills)
        self.assertEqual(result, 100)

    def test_calculate_execution_price_multiple_fills(self):
        """Test calculating execution price with multiple fills."""
        fills = [(100, 30), (101, 20), (102, 50)]
        # Weighted average: (100*30 + 101*20 + 102*50) / 100 = 10120 / 100 = 101
        result = self.simulator.calculate_execution_price(fills)
        self.assertEqual(result, 101)

    def test_calculate_execution_price_with_sell_fills(self):
        """Test calculating execution price with sell fills (negative volumes)."""
        fills = [(100, -30), (101, -20), (102, -50)]
        # Should use absolute volumes for calculation
        result = self.simulator.calculate_execution_price(fills)
        self.assertEqual(result, 101)

    def test_calculate_slippage(self):
        """Test calculating slippage."""
        fills = [(101, 30), (102, 20), (103, 50)]
        reference_price = 100
        # VWAP = (101*30+102*20+103*50)//100 = 10220//100 = 102, slippage = 2
        result = self.simulator.calculate_slippage(fills, reference_price)
        self.assertEqual(result, 2)

    def test_calculate_slippage_empty(self):
        """Test calculating slippage with empty fills."""
        result = self.simulator.calculate_slippage([], 100)
        self.assertEqual(result, 0)

    def test_calculate_slippage_invalid_reference(self):
        """Test calculating slippage with invalid reference price."""
        with self.assertRaises(ValueError):
            self.simulator.calculate_slippage([(100, 50)], 100.5)

    def test_get_unfilled_amount_empty_fills(self):
        """Test getting unfilled amount with no fills."""
        result = self.simulator.get_unfilled_amount(100, [])
        self.assertEqual(result, 100)

    def test_get_unfilled_amount_partial_fill(self):
        """Test getting unfilled amount with partial fill."""
        fills = [(100, 30), (101, 20)]
        result = self.simulator.get_unfilled_amount(100, fills)
        self.assertEqual(result, 50)  # 100 - (30 + 20) = 50

    def test_get_unfilled_amount_sell_order(self):
        """Test getting unfilled amount for sell order."""
        fills = [(100, -30), (99, -20)]
        result = self.simulator.get_unfilled_amount(-100, fills)
        self.assertEqual(result, -50)  # -100 - (-50) = -50

    def test_get_unfilled_amount_full_fill(self):
        """Test getting unfilled amount with full fill."""
        fills = [(100, 50), (101, 50)]
        result = self.simulator.get_unfilled_amount(100, fills)
        self.assertEqual(result, 0)

    def test_fill_order_with_slippage_method(self):
        """Test the convenience fill_order_with_slippage method."""
        market_depth = {
            'bids': [(100, 100)],
            'asks': [(101, 30), (102, 40)]
        }
        result = self.simulator.fill_order_with_slippage(50, market_depth)
        expected = [(101, 30), (102, 20)]
        self.assertEqual(result, expected)

    def test_deterministic_behavior(self):
        """Test that same inputs always produce same outputs."""
        market_depth = {
            'bids': [(100, 50), (99, 50)],
            'asks': [(101, 30), (102, 40)]
        }

        result1 = self.simulator.fill_order(60, market_depth)
        result2 = self.simulator.fill_order(60, market_depth)
        result3 = self.simulator.fill_order(60, market_depth)

        self.assertEqual(result1, result2)
        self.assertEqual(result2, result3)
        self.assertEqual(result1, [(101, 30), (102, 30)])

    def test_large_order_numbers(self):
        """Test with large numbers to ensure integer handling."""
        market_depth = {
            'bids': [(1000000, 1000000000)],
            'asks': [(1000001, 1000000000)]
        }
        result = self.simulator.fill_order(500000000, market_depth)
        self.assertEqual(result, [(1000001, 500000000)])

    def test_negative_price_handling(self):
        """Test with negative prices (edge case)."""
        market_depth = {
            'bids': [(-100, 50)],
            'asks': [(-99, 50)]
        }
        result = self.simulator.fill_order(30, market_depth)
        self.assertEqual(result, [(-99, 30)])


if __name__ == '__main__':
    unittest.main()
