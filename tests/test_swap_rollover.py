import unittest
from tare.microstructure.swap_rollover import SwapRollover


class TestSwapRollover(unittest.TestCase):
    def setUp(self):
        self.swap = SwapRollover()

    # ----- calculate_swap -----
    def test_calculate_swap_normal(self):
        result = self.swap.calculate_swap(1000, 5, 1)
        self.assertIsInstance(result, int)
        self.assertEqual(result, 5000)

    def test_calculate_swap_negative_rate(self):
        result = self.swap.calculate_swap(1000, -2, 3)
        self.assertEqual(result, -6000)

    def test_calculate_swap_zero_position_size(self):
        result = self.swap.calculate_swap(0, 5, 1)
        self.assertEqual(result, 0)

    def test_calculate_swap_days_positive(self):
        result = self.swap.calculate_swap(1000, 5, 7)
        self.assertEqual(result, 35000)

    def test_calculate_swap_days_zero(self):
        with self.assertRaises(ValueError):
            self.swap.calculate_swap(1000, 5, 0)

    def test_calculate_swap_days_negative(self):
        with self.assertRaises(ValueError):
            self.swap.calculate_swap(1000, 5, -1)

    def test_calculate_swap_float_position_size(self):
        with self.assertRaises(ValueError):
            self.swap.calculate_swap(1000.5, 5, 1)

    def test_calculate_swap_float_rate(self):
        with self.assertRaises(ValueError):
            self.swap.calculate_swap(1000, 5.5, 1)

    def test_calculate_swap_float_days(self):
        with self.assertRaises(ValueError):
            self.swap.calculate_swap(1000, 5, 1.0)

    # ----- is_rollover_day -----
    def test_is_rollover_day_wednesday(self):
        result = self.swap.is_rollover_day(2)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

    def test_is_rollover_day_monday(self):
        self.assertFalse(self.swap.is_rollover_day(0))

    def test_is_rollover_day_tuesday(self):
        self.assertFalse(self.swap.is_rollover_day(1))

    def test_is_rollover_day_thursday(self):
        self.assertFalse(self.swap.is_rollover_day(3))

    def test_is_rollover_day_friday(self):
        self.assertFalse(self.swap.is_rollover_day(4))

    def test_is_rollover_day_saturday(self):
        self.assertFalse(self.swap.is_rollover_day(5))

    def test_is_rollover_day_sunday(self):
        self.assertFalse(self.swap.is_rollover_day(6))

    def test_is_rollover_day_out_of_range_low(self):
        with self.assertRaises(ValueError):
            self.swap.is_rollover_day(-1)

    def test_is_rollover_day_out_of_range_high(self):
        with self.assertRaises(ValueError):
            self.swap.is_rollover_day(7)

    def test_is_rollover_day_float_input(self):
        with self.assertRaises(ValueError):
            self.swap.is_rollover_day(2.0)

    # ----- determinism -----
    def test_determinism_calculate_swap(self):
        r1 = self.swap.calculate_swap(1000, 5, 1)
        r2 = self.swap.calculate_swap(1000, 5, 1)
        self.assertEqual(r1, r2)

    def test_determinism_is_rollover_day(self):
        r1 = self.swap.is_rollover_day(2)
        r2 = self.swap.is_rollover_day(2)
        self.assertEqual(r1, r2)

    # ----- return types (sanity) -----
    def test_return_types_calculate_swap(self):
        self.assertIsInstance(self.swap.calculate_swap(10, 2, 1), int)

    def test_return_types_is_rollover_day(self):
        self.assertIsInstance(self.swap.is_rollover_day(2), bool)


if __name__ == "__main__":
    unittest.main()