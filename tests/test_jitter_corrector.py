"""
Test suite for JitterCorrector module.
"""

import unittest
from tare.tick_data_engine.jitter_corrector import JitterCorrector
from tare.time_engine.time_engine import TimeEngine


class TestJitterCorrector(unittest.TestCase):
    """Test cases for JitterCorrector."""

    def setUp(self):
        """Set up test fixtures."""
        self.corrector = JitterCorrector()

    def test_init_default(self):
        """Test initialization with default TimeEngine."""
        corrector = JitterCorrector()
        self.assertIsInstance(corrector.time_engine, TimeEngine)
        self.assertIsNotNone(corrector.tick_cleaner)

    def test_init_custom_time_engine(self):
        """Test initialization with custom TimeEngine."""
        time_engine = TimeEngine(base_latency_ns=2000)
        corrector = JitterCorrector(time_engine=time_engine)
        self.assertEqual(corrector.time_engine.base_latency_ns, 2000)

    def test_correct_timestamps_empty_list(self):
        """Test jitter correction with empty tick list."""
        result = self.corrector.correct_timestamps([], max_jitter_us=10)
        self.assertEqual(result, [])

    def test_correct_timestamps_invalid_max_jitter(self):
        """Test jitter correction with invalid max_jitter_us."""
        with self.assertRaises(ValueError):
            self.corrector.correct_timestamps([], max_jitter_us=-1)

        with self.assertRaises(ValueError):
            self.corrector.correct_timestamps([], max_jitter_us="10")

    def test_correct_timestamps_single_tick(self):
        """Test jitter correction with single tick."""
        ticks = [
            {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100}
        ]
        result = self.corrector.correct_timestamps(ticks, max_jitter_us=10)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['timestamp'], 1000000000)

    def test_correct_timestamps_no_jitter(self):
        """Test jitter correction when no ticks are within jitter threshold."""
        ticks = [
            {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1000010000, 'symbol': 'AAPL', 'price': 15001, 'volume': 200},
            {'timestamp': 1000020000, 'symbol': 'AAPL', 'price': 15002, 'volume': 300}
        ]
        result = self.corrector.correct_timestamps(ticks, max_jitter_us=1)  # 1us = 1000ns
        # All timestamps are > 1000ns apart, so no correction
        self.assertEqual([t['timestamp'] for t in result], [1000000000, 1000010000, 1000020000])

    def test_correct_timestamps_with_jitter(self):
        """Test jitter correction when ticks are within jitter threshold."""
        ticks = [
            {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1000000500, 'symbol': 'AAPL', 'price': 15001, 'volume': 200},  # 500ns later
            {'timestamp': 1000000900, 'symbol': 'AAPL', 'price': 15002, 'volume': 300},  # 400ns later
            {'timestamp': 1000020000, 'symbol': 'AAPL', 'price': 15003, 'volume': 400}   # 1100ns later
        ]
        # max_jitter_us = 1us = 1000ns
        result = self.corrector.correct_timestamps(ticks, max_jitter_us=1)
        
        # First tick: 1000000000
        # Second tick: 500ns diff < 1000ns, corrected to 1000000000
        # Third tick: 400ns diff from previous corrected (1000000000) < 1000ns, corrected to 1000000000
        # Fourth tick: 1000ns diff from previous corrected (1000000000) == 1000ns, NOT corrected (strict >)
        expected_timestamps = [1000000000, 1000000000, 1000000000, 1000020000]
        self.assertEqual([t['timestamp'] for t in result], expected_timestamps)

    def test_correct_timestamps_unsorted_input(self):
        """Test jitter correction with unsorted input (should be sorted first)."""
        ticks = [
            {'timestamp': 1000002000, 'symbol': 'AAPL', 'price': 15002, 'volume': 300},
            {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1000001000, 'symbol': 'AAPL', 'price': 15001, 'volume': 200}
        ]
        result = self.corrector.correct_timestamps(ticks, max_jitter_us=10)
        # Should be sorted first, then jitter correction applied
        self.assertEqual(result[0]['timestamp'], 1000000000)
        # 1000ns < 10us → snapped
        self.assertEqual(result[1]['timestamp'], 1000000000)
        # 2000ns < 10us → snapped
        self.assertEqual(result[2]['timestamp'], 1000000000)

    def test_correct_timestamps_cleans_ticks(self):
        """Test that jitter correction cleans ticks first."""
        ticks = [
            {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': -100, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},  # Invalid
            {'timestamp': 1000001000, 'symbol': 'AAPL', 'price': 15001, 'volume': 200},
            {'timestamp': 1000001000, 'symbol': 'AAPL', 'price': 15001, 'volume': 200},  # Duplicate
            {'timestamp': 1000002000, 'symbol': '', 'price': 15002, 'volume': 300},  # Empty symbol
        ]
        result = self.corrector.correct_timestamps(ticks, max_jitter_us=10)
        # Only valid, unique ticks should remain
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['timestamp'], 1000000000)
        # 1000ns < 10us=10000ns → snapped to previous
        self.assertEqual(result[1]['timestamp'], 1000000000)

    def test_synchronize_sources_empty(self):
        """Test synchronization with empty input."""
        result = self.corrector.synchronize_sources({})
        self.assertEqual(result, [])

    def test_synchronize_sources_single_source(self):
        """Test synchronization with single source."""
        ticks_by_source = {
            'exchange1': [
                {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
                {'timestamp': 1000001000, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50}
            ]
        }
        result = self.corrector.synchronize_sources(ticks_by_source)
        
        self.assertEqual(len(result), 2)
        
        # Check structure
        for tick in result:
            self.assertIn('source', tick)
            self.assertIn('market_time', tick)
            self.assertIn('local_time', tick)
            self.assertIn('simulation_time', tick)
            self.assertIn('sequence', tick)
            self.assertEqual(tick['source'], 'exchange1')
        
        # Check ordering
        self.assertEqual(result[0]['market_time'], 1000000000)
        self.assertEqual(result[1]['market_time'], 1000001000)
        
        # Check TimeEngine fields
        self.assertEqual(result[0]['sequence'], 1)
        self.assertEqual(result[1]['sequence'], 2)
        self.assertEqual(result[0]['simulation_time'], 1)
        self.assertEqual(result[1]['simulation_time'], 2)

    def test_synchronize_sources_multiple_sources(self):
        """Test synchronization with multiple sources."""
        ticks_by_source = {
            'exchange1': [
                {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
                {'timestamp': 1000002000, 'symbol': 'AAPL', 'price': 15001, 'volume': 200}
            ],
            'exchange2': [
                {'timestamp': 1000001000, 'symbol': 'AAPL', 'price': 15000, 'volume': 150},
                {'timestamp': 1000001500, 'symbol': 'AAPL', 'price': 15000, 'volume': 250}
            ]
        }
        result = self.corrector.synchronize_sources(ticks_by_source)
        
        self.assertEqual(len(result), 4)
        
        # Check ordering by market_time
        market_times = [t['market_time'] for t in result]
        self.assertEqual(market_times, [1000000000, 1000001000, 1000001500, 1000002000])
        
        # Check sources
        sources = [t['source'] for t in result]
        self.assertEqual(sources, ['exchange1', 'exchange2', 'exchange2', 'exchange1'])
        
        # Check TimeEngine sequence
        sequences = [t['sequence'] for t in result]
        self.assertEqual(sequences, [1, 2, 3, 4])

    def test_synchronize_sources_applies_jitter_correction(self):
        """Test that synchronization applies jitter correction to each source."""
        ticks_by_source = {
            'exchange1': [
                {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
                {'timestamp': 1000000500, 'symbol': 'AAPL', 'price': 15001, 'volume': 200},  # Within 10us jitter
            ]
        }
        result = self.corrector.synchronize_sources(ticks_by_source)
        
        # market_time = original timestamp (jitter affects ordering, not market_time)
        self.assertEqual(result[0]['market_time'], 1000000000)
        self.assertEqual(result[1]['market_time'], 1000000500)

    def test_synchronize_sources_invalid_input(self):
        """Test synchronization with invalid input."""
        with self.assertRaises(ValueError):
            self.corrector.synchronize_sources("not a dict")
        
        with self.assertRaises(ValueError):
            self.corrector.synchronize_sources({123: []})  # Non-string source name
        
        with self.assertRaises(ValueError):
            self.corrector.synchronize_sources({'exchange1': "not a list"})

    def test_correct_timestamps_generator(self):
        """Test jitter correction with generator input."""
        def tick_generator():
            ticks = [
                {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
                {'timestamp': 1000000500, 'symbol': 'AAPL', 'price': 15001, 'volume': 200},
                {'timestamp': 1000000900, 'symbol': 'AAPL', 'price': 15002, 'volume': 300},
            ]
            for tick in ticks:
                yield tick
        
        generator = tick_generator()
        result = list(self.corrector.correct_timestamps_generator(generator, max_jitter_us=1))
        
        # With 1us = 1000ns threshold, all ticks should be corrected to first timestamp
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['timestamp'], 1000000000)
        self.assertEqual(result[1]['timestamp'], 1000000000)
        self.assertEqual(result[2]['timestamp'], 1000000000)

    def test_correct_timestamps_generator_invalid_jitter(self):
        """Test generator jitter correction with invalid max_jitter_us."""
        def empty_generator():
            return
            yield
        
        with self.assertRaises(ValueError):
            list(self.corrector.correct_timestamps_generator(empty_generator(), max_jitter_us=-1))

    def test_determinism(self):
        """Test that same input always produces same output."""
        ticks = [
            {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1000000500, 'symbol': 'AAPL', 'price': 15001, 'volume': 200},
            {'timestamp': 1000000900, 'symbol': 'AAPL', 'price': 15002, 'volume': 300},
        ]
        
        result1 = self.corrector.correct_timestamps(ticks, max_jitter_us=10)
        
        # Create new corrector to ensure no state carried over
        new_corrector = JitterCorrector()
        result2 = new_corrector.correct_timestamps(ticks, max_jitter_us=10)
        
        self.assertEqual(result1, result2)

    def test_only_int_values(self):
        """Test that all values in output are integers."""
        ticks = [
            {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1000001000, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50}
        ]
        
        result = self.corrector.synchronize_sources({'exchange1': ticks})
        
        for tick in result:
            for key, value in tick.items():
                if key != 'symbol' and key != 'source':  # These are strings
                    self.assertIsInstance(value, int, f"Key '{key}' has non-int value: {value}")

    def test_tie_breaking_with_time_engine(self):
        """Test that TimeEngine provides deterministic tie-breaking."""
        # Create ticks with same timestamp
        ticks = [
            {'timestamp': 1000000000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 1000000000, 'symbol': 'GOOGL', 'price': 280000, 'volume': 50},
            {'timestamp': 1000000000, 'symbol': 'MSFT', 'price': 35000, 'volume': 200}
        ]
        
        result = self.corrector.synchronize_sources({'exchange1': ticks})
        
        # All should have same market_time but different sequence numbers
        self.assertEqual(result[0]['market_time'], 1000000000)
        self.assertEqual(result[1]['market_time'], 1000000000)
        self.assertEqual(result[2]['market_time'], 1000000000)
        
        # Sequence numbers should be 1, 2, 3
        self.assertEqual(result[0]['sequence'], 1)
        self.assertEqual(result[1]['sequence'], 2)
        self.assertEqual(result[2]['sequence'], 3)
        
        # Simulation times should be 1, 2, 3
        self.assertEqual(result[0]['simulation_time'], 1)
        self.assertEqual(result[1]['simulation_time'], 2)
        self.assertEqual(result[2]['simulation_time'], 3)


if __name__ == '__main__':
    unittest.main()
