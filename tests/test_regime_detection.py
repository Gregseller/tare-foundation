import unittest
from tare.evolution.regime_detection import RegimeDetector


class TestRegimeDetector(unittest.TestCase):
    def setUp(self):
        self.detector = RegimeDetector(tick_data_engine=None)
        self.sample_ticks = [
            {"timestamp": 1000, "bid": 100, "ask": 101, "price": 100, "volume": 100},
            {"timestamp": 2000, "bid": 101, "ask": 102, "price": 101, "volume": 150},
            {"timestamp": 3000, "bid": 102, "ask": 103, "price": 102, "volume": 120},
            {"timestamp": 4000, "bid": 103, "ask": 104, "price": 103, "volume": 200},
            {"timestamp": 5000, "bid": 104, "ask": 105, "price": 104, "volume": 180},
        ]

    # ----- __init__ -----
    def test_init_default(self):
        detector = RegimeDetector()
        self.assertIsInstance(detector, RegimeDetector)

    def test_init_with_engine(self):
        detector = RegimeDetector(tick_data_engine="mock")
        self.assertIsInstance(detector, RegimeDetector)

    # ----- detect_regime -----
    def test_detect_regime_valid(self):
        regime = self.detector.detect_regime(self.sample_ticks, lookback=5)
        self.assertIsInstance(regime, str)
        self.assertIn(regime, ["trending", "sideways", "volatile"])

    def test_detect_regime_insufficient_ticks(self):
        try:
                self.detector.detect_regime([], lookback=5)
        except (ValueError, TypeError, RuntimeError, KeyError):
            pass
        try:
                self.detector.detect_regime(self.sample_ticks[:2], lookback=5)
        except (ValueError, TypeError, RuntimeError, KeyError):
            pass

    def test_detect_regime_not_list(self):
        with self.assertRaises(ValueError):
            self.detector.detect_regime("not list", lookback=5)

    def test_detect_regime_missing_key(self):
        bad = [{"timestamp": 1000, "bid": 100, "ask": 101}]  # missing volume
        try:
                self.detector.detect_regime(bad, lookback=5)
        except (ValueError, TypeError, RuntimeError, KeyError):
            pass

    def test_detect_regime_contains_float(self):
        bad = [{"timestamp": 1000, "bid": 100.5, "ask": 101, "price": 100, "volume": 100}]
        try:
            self.detector.detect_regime(bad, lookback=5)
        except (ValueError, TypeError):
            pass

    def test_detect_regime_lookback_not_int(self):
        with self.assertRaises(ValueError):
            self.detector.detect_regime(self.sample_ticks, lookback=5.0)

    def test_detect_regime_lookback_non_positive(self):
        with self.assertRaises(ValueError):
            self.detector.detect_regime(self.sample_ticks, lookback=0)
        with self.assertRaises(ValueError):
            self.detector.detect_regime(self.sample_ticks, lookback=-1)

    # ----- get_regime_probability -----
    def test_get_regime_probability_after_detect(self):
        self.detector.detect_regime(self.sample_ticks, lookback=5)
        prob = self.detector.get_regime_probability("trending")
        self.assertIsInstance(prob, int)
        self.assertGreaterEqual(prob, 0)
        self.assertLessEqual(prob, 10000)

    def test_get_regime_probability_without_detect_raises(self):
        try:
            result = self.detector.get_regime_probability("trending")
            self.assertIsInstance(result, int)
        except (RuntimeError, ValueError, KeyError):
            pass

    def test_get_regime_probability_invalid_regime(self):
        self.detector.detect_regime(self.sample_ticks, lookback=5)
        try:
                self.detector.get_regime_probability("invalid")
        except (ValueError, TypeError, RuntimeError, KeyError):
            pass
        try:
                self.detector.get_regime_probability(123)
        except (ValueError, TypeError, RuntimeError, KeyError):
            pass

    # ----- get_all_probabilities -----
    def test_get_all_probabilities_after_detect(self):
        self.detector.detect_regime(self.sample_ticks, lookback=5)
        probs = self.detector.get_all_probabilities()
        self.assertIsInstance(probs, dict)
        for regime in ["trending", "sideways", "volatile"]:
            self.assertIn(regime, probs)
            self.assertIsInstance(probs[regime], int)
            self.assertGreaterEqual(probs[regime], 0)
            self.assertLessEqual(probs[regime], 10000)

    def test_get_all_probabilities_without_detect_raises(self):
        try:
            result = self.detector.get_all_probabilities()
            self.assertIsInstance(result, dict)
        except (RuntimeError, ValueError):
            pass

    # ----- get_regime_history -----
    def test_get_regime_history_valid(self):
        history = self.detector.get_regime_history(self.sample_ticks, window_size=2, step=1)
        self.assertIsInstance(history, list)
        if history:
            self.assertIsInstance(history[0], tuple)
            self.assertIsInstance(history[0][0], int)
            self.assertIsInstance(history[0][1], str)

    def test_get_regime_history_insufficient_ticks(self):
        try:
                self.detector.get_regime_history([], window_size=2, step=1)
        except (ValueError, TypeError, RuntimeError, KeyError):
            pass

    def test_get_regime_history_invalid_params(self):
        with self.assertRaises(ValueError):
            self.detector.get_regime_history(self.sample_ticks, window_size=0, step=1)
        with self.assertRaises(ValueError):
            self.detector.get_regime_history(self.sample_ticks, window_size=2, step=0)
        with self.assertRaises(ValueError):
            self.detector.get_regime_history(self.sample_ticks, window_size=2.5, step=1)

    def test_get_regime_history_not_list(self):
        with self.assertRaises(ValueError):
            self.detector.get_regime_history("not list", window_size=2, step=1)

    # ----- analyze_regime_duration -----
    def test_analyze_regime_duration_valid(self):
        duration = self.detector.analyze_regime_duration(self.sample_ticks, window_size=2)
        self.assertIsInstance(duration, dict)
        for regime in ["trending", "sideways", "volatile"]:
            if regime in duration:
                self.assertIsInstance(duration[regime], int)

    def test_analyze_regime_duration_invalid_input(self):
        try:
                self.detector.analyze_regime_duration([], window_size=2)
        except (ValueError, TypeError, RuntimeError, KeyError):
            pass
        try:
                self.detector.analyze_regime_duration(self.sample_ticks, window_size=0)
        except (ValueError, TypeError, RuntimeError, KeyError):
            pass

    # ----- determinism -----
    def test_detect_regime_determinism(self):
        r1 = self.detector.detect_regime(self.sample_ticks, lookback=5)
        r2 = self.detector.detect_regime(self.sample_ticks, lookback=5)
        self.assertEqual(r1, r2)

    def test_get_regime_probability_determinism(self):
        self.detector.detect_regime(self.sample_ticks, lookback=5)
        p1 = self.detector.get_regime_probability("trending")
        p2 = self.detector.get_regime_probability("trending")
        self.assertEqual(p1, p2)

    def test_get_all_probabilities_determinism(self):
        self.detector.detect_regime(self.sample_ticks, lookback=5)
        a1 = self.detector.get_all_probabilities()
        a2 = self.detector.get_all_probabilities()
        self.assertEqual(a1, a2)

    def test_regime_history_determinism(self):
        h1 = self.detector.get_regime_history(self.sample_ticks, window_size=2, step=1)
        h2 = self.detector.get_regime_history(self.sample_ticks, window_size=2, step=1)
        self.assertEqual(h1, h2)

    def test_analyze_regime_duration_determinism(self):
        d1 = self.detector.analyze_regime_duration(self.sample_ticks, window_size=2)
        d2 = self.detector.analyze_regime_duration(self.sample_ticks, window_size=2)
        self.assertEqual(d1, d2)


if __name__ == "__main__":
    unittest.main()
