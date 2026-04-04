"""
Tests for regime_detection module.
"""

import unittest
from tare.evolution.regime_detection import RegimeDetector


class MockTickEngine:
    """Mock TickDataEngine for testing."""
    
    def __init__(self):
        self.clean_ticks = []
    
    def get_clean_ticks(self):
        return self.clean_ticks


class TestRegimeDetector(unittest.TestCase):
    """Test RegimeDetector class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.engine = MockTickEngine()
        self.detector = RegimeDetector(self.engine)
    
    def test_init_with_none_engine(self):
        """Test initialization rejects None engine."""
        with self.assertRaises(ValueError):
            RegimeDetector(None)
    
    def test_detect_regime_invalid_ticks_type(self):
        """Test detect_regime rejects non-list ticks."""
        with self.assertRaises(ValueError):
            self.detector.detect_regime("not_a_list", 5)
    
    def test_detect_regime_invalid_lookback_type(self):
        """Test detect_regime rejects non-int lookback."""
        ticks = [{'price': 100}]
        with self.assertRaises(ValueError):
            self.detector.detect_regime(ticks, 1.5)
    
    def test_detect_regime_negative_lookback(self):
        """Test detect_regime rejects negative lookback."""
        ticks = [{'price': 100}]
        with self.assertRaises(ValueError):
            self.detector.detect_regime(ticks, -1)
    
    def test_detect_regime_insufficient_ticks(self):
        pass

