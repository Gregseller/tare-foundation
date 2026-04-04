import unittest
from tare.validation.adequacy_v1 import AdequacyV1


class TestAdequacyV1(unittest.TestCase):
    def setUp(self):
        self.v = AdequacyV1()

    def test_init_default(self):
        self.assertEqual(self.v._min_ticks, 1000)

    def test_init_invalid(self):
        with self.assertRaises(ValueError): AdequacyV1(min_ticks=0)
        with self.assertRaises(ValueError): AdequacyV1(min_ticks=-1)
        with self.assertRaises(ValueError): AdequacyV1(min_ticks=1.5)
        with self.assertRaises(ValueError): AdequacyV1(max_gap_ns=0)

    def test_check_min_ticks(self):
        self.assertTrue(self.v.check_min_ticks(1000))
        self.assertFalse(self.v.check_min_ticks(999))
        with self.assertRaises(ValueError): self.v.check_min_ticks(1.5)

    def test_check_max_gap_empty(self):
        self.assertTrue(self.v.check_max_gap([]))
        self.assertTrue(self.v.check_max_gap([1000]))

    def test_check_max_gap_ok(self):
        self.assertTrue(self.v.check_max_gap([1000, 2000, 3000]))

    def test_check_max_gap_exceeds(self):
        self.assertFalse(self.v.check_max_gap([1000, 1000 + 3_600_000_000_001]))

    def test_check_max_gap_invalid(self):
        with self.assertRaises(ValueError): self.v.check_max_gap("bad")
        with self.assertRaises(ValueError): self.v.check_max_gap([1000, 2.5])

    def test_check_spread_sanity_empty(self):
        self.assertTrue(self.v.check_spread_sanity([], 100))

    def test_check_spread_sanity_ok(self):
        self.assertTrue(self.v.check_spread_sanity([10, 20, 30], 50))

    def test_check_spread_sanity_negative(self):
        self.assertFalse(self.v.check_spread_sanity([-1, 20], 50))

    def test_check_spread_sanity_exceeds(self):
        self.assertFalse(self.v.check_spread_sanity([10, 60], 50))

    def test_check_spread_sanity_invalid(self):
        with self.assertRaises(ValueError): self.v.check_spread_sanity("bad", 50)
        with self.assertRaises(ValueError): self.v.check_spread_sanity([10], 0)

    def test_get_summary_adequate(self):
        s = self.v.get_summary(2000, [1000, 2000], [10, 20], 100)
        self.assertTrue(s["adequate"])
        self.assertIn("tick_count", s)

    def test_get_summary_not_adequate(self):
        s = self.v.get_summary(500, [1000, 2000], [10, 20], 100)
        self.assertFalse(s["adequate"])

    def test_determinism(self):
        r1 = self.v.get_summary(2000, [1000, 2000], [10], 100)
        r2 = self.v.get_summary(2000, [1000, 2000], [10], 100)
        self.assertEqual(r1, r2)

if __name__ == "__main__":
    unittest.main()
