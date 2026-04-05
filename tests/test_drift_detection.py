import unittest

from tare.evolution.drift_detection import DriftDetector as DriftDetection

class TestDriftDetection(unittest.TestCase):

    def setUp(self):

        self.detector = DriftDetection()

        self.live_data = {

            "returns": [100, 110, 120, 130, 140],

            "volumes": [1000, 1100, 1200, 1150, 1250],

            "spreads": [10, 11, 12, 12, 11],

            "volatility": 500,

            "drawdown": 200,

            "fill_quality": [5, 6, 7, 6, 5],

            "trade_frequency": 10

        }

        self.baseline = {

            "returns": [100, 100, 100, 100, 100],

            "volumes": [1000, 1000, 1000, 1000, 1000],

            "spreads": [10, 10, 10, 10, 10],

            "volatility": 400,

            "drawdown": 150,

            "fill_quality": [5, 5, 5, 5, 5],

            "trade_frequency": 10

        }

    def test_init_default(self):

        self.assertIsInstance(DriftDetection(), DriftDetection)

    def test_detect_valid(self):

        result = self.detector.detect(self.live_data, self.baseline)

        self.assertIsInstance(result, dict)

        self.assertIn("returns_drift", result)

        self.assertIn("volumes_drift", result)

        self.assertIn("spreads_drift", result)

        self.assertIn("overall_drift_score", result)

        for v in result.values():

            self.assertIsInstance(v, int)

    def test_detect_not_dict(self):

        with self.assertRaises((ValueError, TypeError)):

            self.detector.detect("not dict", self.baseline)

    def test_detect_determinism(self):

        r1 = self.detector.detect(self.live_data, self.baseline)

        r2 = self.detector.detect(self.live_data, self.baseline)

        self.assertEqual(r1, r2)

    def test_is_drifted_after_detect(self):

        self.detector.detect(self.live_data, self.baseline)

        result = self.detector.is_drifted(threshold=100)

        self.assertIsInstance(result, bool)

    def test_is_drifted_invalid_threshold(self):
        self.detector.detect(self.live_data, self.baseline)
        with self.assertRaises((ValueError, TypeError)):
            self.detector.is_drifted(-5)
        with self.assertRaises((ValueError, TypeError)):
            self.detector.is_drifted(10001)
        with self.assertRaises((ValueError, TypeError)):
            self.detector.is_drifted(100.5)
        with self.assertRaises((ValueError, TypeError)):

            self.detector.is_drifted(-5)

    def test_is_drifted_determinism(self):

        self.detector.detect(self.live_data, self.baseline)

        self.assertEqual(

            self.detector.is_drifted(100),

            self.detector.is_drifted(100)

        )

    def test_get_drift_metrics_after_detect(self):

        self.detector.detect(self.live_data, self.baseline)

        metrics = self.detector.get_drift_metrics()

        self.assertIsInstance(metrics, dict)

        for v in metrics.values():

            self.assertIsInstance(v, int)

    def test_get_drift_metrics_determinism(self):

        self.detector.detect(self.live_data, self.baseline)

        self.assertEqual(

            self.detector.get_drift_metrics(),

            self.detector.get_drift_metrics()

        )

    def test_regime_signature_valid(self):

        sig = self.detector.regime_signature({"prices": [1,2,3]})

        self.assertIsInstance(sig, str)

        self.assertGreater(len(sig), 0)

    def test_regime_signature_invalid(self):

        with self.assertRaises((ValueError, TypeError)):

            self.detector.regime_signature("not dict")

    def test_regime_signature_determinism(self):

        data = {"prices": [1,2,3], "volumes": [100,200,300]}

        self.assertEqual(

            self.detector.regime_signature(data),

            self.detector.regime_signature(data)

        )

if __name__ == "__main__":

    unittest.main()

