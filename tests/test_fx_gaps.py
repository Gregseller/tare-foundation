import unittest
from tare.microstructure.fx_gaps import FXGaps
class TestFXGaps(unittest.TestCase):
    def setUp(self):
        self.fx = FXGaps()
    def test_detect_gap_up(self):
        self.assertEqual(self.fx.detect_gap(1000, 1010), 10)
    def test_detect_gap_down(self):
        self.assertEqual(self.fx.detect_gap(1000, 990), -10)
    def test_detect_gap_zero(self):
        self.assertEqual(self.fx.detect_gap(1000, 1000), 0)
    def test_detect_gap_float_prev_close(self):
        with self.assertRaises(ValueError): self.fx.detect_gap(1000.5, 1010)
    def test_detect_gap_float_current_open(self):
        with self.assertRaises(ValueError): self.fx.detect_gap(1000, 1010.5)
    def test_is_significant_above(self):
        self.assertTrue(self.fx.is_significant(15, 10))
    def test_is_significant_equal(self):
        self.assertTrue(self.fx.is_significant(10, 10))
    def test_is_significant_below(self):
        self.assertFalse(self.fx.is_significant(5, 10))
    def test_is_significant_negative_gap(self):
        self.assertTrue(self.fx.is_significant(-15, 10))
    def test_is_significant_float(self):
        with self.assertRaises(ValueError): self.fx.is_significant(15.5, 10)
    def test_is_significant_threshold_zero(self):
        with self.assertRaises(ValueError): self.fx.is_significant(10, 0)
    def test_adjust_positive(self):
        self.assertEqual(self.fx.adjust_for_gap(1000, 10), 1010)
    def test_adjust_negative(self):
        self.assertEqual(self.fx.adjust_for_gap(1000, -10), 990)
    def test_adjust_float(self):
        with self.assertRaises(ValueError): self.fx.adjust_for_gap(1000.5, 10)
    def test_determinism(self):
        self.assertEqual(self.fx.detect_gap(1000,1010), self.fx.detect_gap(1000,1010))
    def test_return_types(self):
        self.assertIsInstance(self.fx.detect_gap(100, 101), int)
        self.assertIsInstance(self.fx.is_significant(5, 10), bool)
        self.assertIsInstance(self.fx.adjust_for_gap(100, 5), int)
if __name__ == "__main__":
    unittest.main()
