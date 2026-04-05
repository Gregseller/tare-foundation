import unittest
from tare.validation.adequacy_v2 import AdequacyV2


class TestAdequacyV2(unittest.TestCase):
    def setUp(self):
        self.validator = AdequacyV2()

    # ----- __init__ -----
    def test_init_default(self):
        validator = AdequacyV2()
        self.assertIsInstance(validator, AdequacyV2)

    def test_init_with_adequacy_v1(self):
        from tare.validation.adequacy_v1 import AdequacyV1
        v1 = AdequacyV1()
        validator = AdequacyV2(adequacy_v1=v1)
        self.assertIsInstance(validator, AdequacyV2)

    # ----- ks_test -----
    def test_ks_test_identical_lists(self):
        sample = [1, 2, 3, 4, 5]
        result = self.validator.ks_test(sample, sample)
        self.assertIsInstance(result, dict)
        self.assertIn("statistic", result)
        self.assertIn("adequate", result)
        self.assertIsInstance(result["statistic"], int)
        self.assertIsInstance(result["adequate"], bool)
        self.assertEqual(result["statistic"], 0)
        self.assertTrue(result["adequate"])

    def test_ks_test_different_lists(self):
        sample1 = [1, 2, 3, 4, 5]
        sample2 = [1, 2, 3, 4, 6]
        result = self.validator.ks_test(sample1, sample2)
        self.assertIsInstance(result["statistic"], int)
        self.assertIsInstance(result["adequate"], bool)
        # statistic > 0, adequate may be False depending on implementation
        self.assertGreater(result["statistic"], 0)

    def test_ks_test_empty_list(self):
        with self.assertRaises(ValueError):
            self.validator.ks_test([], [1, 2, 3])
        with self.assertRaises(ValueError):
            self.validator.ks_test([1, 2, 3], [])

    def test_ks_test_contains_float(self):
        with self.assertRaises(ValueError):
            self.validator.ks_test([1.5, 2, 3], [1, 2, 3])
        with self.assertRaises(ValueError):
            self.validator.ks_test([1, 2, 3], [1, 2.5, 3])

    def test_ks_test_non_list(self):
        with self.assertRaises(ValueError):
            self.validator.ks_test("not list", [1, 2, 3])
        with self.assertRaises(ValueError):
            self.validator.ks_test([1, 2, 3], "not list")

    def test_ks_test_determinism(self):
        sample1 = [1, 2, 3, 4, 5]
        sample2 = [1, 2, 3, 4, 6]
        r1 = self.validator.ks_test(sample1, sample2)
        r2 = self.validator.ks_test(sample1, sample2)
        self.assertEqual(r1, r2)

    # ----- fx_adequacy_check -----
    def test_fx_adequacy_check_valid(self):
        fx_pairs = {
            "EURUSD": {
                "bids": [100, 101, 102],
                "asks": [103, 104, 105],
                "volumes": [1000, 2000, 3000]
            },
            "GBPUSD": {
                "bids": [150, 151],
                "asks": [152, 153],
                "volumes": [500, 600]
            }
        }
        result = self.validator.fx_adequacy_check(fx_pairs)
        self.assertIsInstance(result, dict)
        self.assertIsInstance(result, dict)
        for v in result.values():
            self.assertIsInstance(v, bool)

    def test_fx_adequacy_check_empty_dict(self):
        result = self.validator.fx_adequacy_check({})
        self.assertIsInstance(result, dict)

    def test_fx_adequacy_check_missing_keys(self):
        fx_pairs = {
            "EURUSD": {
                "bids": [100, 101],
                "asks": [103, 104]
                # missing "volumes"
            }
        }
        with self.assertRaises(ValueError):
            self.validator.fx_adequacy_check(fx_pairs)

    def test_fx_adequacy_check_contains_float(self):
        fx_pairs = {
            "EURUSD": {
                "bids": [100, 101.5],
                "asks": [103, 104],
                "volumes": [1000, 2000]
            }
        }
        with self.assertRaises(ValueError):
            self.validator.fx_adequacy_check(fx_pairs)

    def test_fx_adequacy_check_non_dict(self):
        with self.assertRaises(ValueError):
            self.validator.fx_adequacy_check("not dict")

    def test_fx_adequacy_check_determinism(self):
        fx_pairs = {
            "EURUSD": {
                "bids": [100, 101],
                "asks": [103, 104],
                "volumes": [1000, 2000]
            }
        }
        r1 = self.validator.fx_adequacy_check(fx_pairs)
        r2 = self.validator.fx_adequacy_check(fx_pairs)
        self.assertEqual(r1, r2)


if __name__ == "__main__":
    unittest.main()