"""
Unit tests for LODEngine module.
"""

import unittest
from tare.microstructure.lod_engine import LODEngine


class TestLODEngine(unittest.TestCase):
    """Test cases for LODEngine class."""

    def setUp(self):
        """Set up test fixtures."""
        self.engine = LODEngine()

        # Sample tick data for testing
        self.sample_ticks = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'AAPL', 'price': 15100, 'volume': 200},
            {'timestamp': 3000, 'symbol': 'AAPL', 'price': 15000, 'volume': 150},
            {'timestamp': 4000, 'symbol': 'AAPL', 'price': 15200, 'volume': 300},
            {'timestamp': 5000, 'symbol': 'AAPL', 'price': 15100, 'volume': 250},
            {'timestamp': 6000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 7000, 'symbol': 'AAPL', 'price': 15300, 'volume': 400},
            {'timestamp': 8000, 'symbol': 'AAPL', 'price': 15200, 'volume': 200},
            {'timestamp': 9000, 'symbol': 'AAPL', 'price': 15100, 'volume': 150},
            {'timestamp': 10000, 'symbol': 'AAPL', 'price': 15000, 'volume': 200},
        ]

    def test_compute_lod1_basic(self):
        """Test basic LOD1 computation."""
        result = self.engine.compute_lod1(self.sample_ticks)

        # Check structure
        self.assertIn('symbol', result)
        self.assertIn('price_levels', result)
        self.assertIn('total_volume', result)
        self.assertIn('total_trades', result)
        self.assertIn('timestamp_range', result)

        # Check values
        self.assertEqual(result['symbol'], 'AAPL')
        self.assertEqual(result['total_trades'], 10)
        self.assertEqual(result['timestamp_range'], (1000, 10000))

        # Check total volume
        total_volume = sum(tick['volume'] for tick in self.sample_ticks)
        self.assertEqual(result['total_volume'], total_volume)

        # Check price levels
        price_levels = result['price_levels']
        self.assertEqual(len(price_levels), 4)  # 15000, 15100, 15200, 15300

        # Verify price 15000
        level_15000 = next(level for level in price_levels if level['price'] == 15000)
        self.assertEqual(level_15000['total_volume'], 550)  # 100 + 150 + 100 + 200
        self.assertEqual(level_15000['trade_count'], 4)
        self.assertEqual(level_15000['first_timestamp'], 1000)
        self.assertEqual(level_15000['last_timestamp'], 10000)

    def test_compute_lod1_empty_list(self):
        """Test LOD1 computation with empty tick list."""
        with self.assertRaises(ValueError):
            self.engine.compute_lod1([])

    def test_compute_lod1_invalid_tick(self):
        """Test LOD1 computation with invalid tick structure."""
        invalid_ticks = [
            {'timestamp': 1000, 'price': 15000, 'volume': 100},  # Missing symbol
        ]
        with self.assertRaises(ValueError):
            self.engine.compute_lod1(invalid_ticks)

    def test_compute_lod2_basic(self):
        """Test basic LOD2 computation."""
        result = self.engine.compute_lod2(self.sample_ticks)

        # Check structure
        self.assertIn('symbol', result)
        self.assertIn('price_levels', result)
        self.assertIn('total_volume', result)
        self.assertIn('total_trades', result)
        self.assertIn('timestamp_range', result)

        # Check values
        self.assertEqual(result['symbol'], 'AAPL')
        self.assertEqual(result['total_trades'], 10)

        # Check price levels
        price_levels = result['price_levels']
        self.assertEqual(len(price_levels), 4)

        # Verify each price level has volume buckets
        for level in price_levels:
            self.assertIn('price', level)
            self.assertIn('volume_buckets', level)
            self.assertIn('total_volume', level)
            self.assertIn('total_trades', level)

            # Should have 5 volume buckets
            self.assertEqual(len(level['volume_buckets']), 5)

            # Check bucket structure
            for bucket in level['volume_buckets']:
                self.assertIn('bucket_start', bucket)
                self.assertIn('bucket_end', bucket)
                self.assertIn('volume', bucket)
                self.assertIn('trade_count', bucket)

    def test_compute_lod2_single_tick(self):
        """Test LOD2 computation with single tick."""
        single_tick = [{'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100}]
        result = self.engine.compute_lod2(single_tick)

        self.assertEqual(result['symbol'], 'AAPL')
        self.assertEqual(result['total_trades'], 1)
        self.assertEqual(result['total_volume'], 100)

        price_levels = result['price_levels']
        self.assertEqual(len(price_levels), 1)

        level = price_levels[0]
        self.assertEqual(level['price'], 15000)
        self.assertEqual(level['total_volume'], 100)
        self.assertEqual(level['total_trades'], 1)

        # Should still have 5 buckets
        self.assertEqual(len(level['volume_buckets']), 5)

    def test_compute_lod3_basic(self):
        """Test basic LOD3 computation."""
        result = self.engine.compute_lod3(self.sample_ticks)

        # Check structure
        self.assertIn('symbol', result)
        self.assertIn('bids', result)
        self.assertIn('asks', result)
        self.assertIn('mid_price', result)
        self.assertIn('spread', result)
        self.assertIn('total_volume', result)
        self.assertIn('total_trades', result)
        self.assertIn('timestamp_range', result)

        # Check values
        self.assertEqual(result['symbol'], 'AAPL')
        self.assertEqual(result['total_trades'], 10)

        # Check bids and asks
        bids = result['bids']
        asks = result['asks']

        # With prices 15000, 15100, 15200, 15300 and median 15100:
        # Bids: 15000, 15100 (<= 15100)
        # Asks: 15200, 15300 (> 15100)
        self.assertEqual(len(bids), 2)
        self.assertEqual(len(asks), 2)

        # Bids should be sorted descending
        self.assertEqual(bids[0]['price'], 15100)
        self.assertEqual(bids[1]['price'], 15000)

        # Asks should be sorted ascending
        self.assertEqual(asks[0]['price'], 15200)
        self.assertEqual(asks[1]['price'], 15300)

        # Check spread
        self.assertEqual(result['spread'], 100)  # 15200 - 15100

        # Check bid/ask structure
        for bid in bids:
            self.assertIn('price', bid)
            self.assertIn('total_volume', bid)
            self.assertIn('remaining_volume', bid)
            self.assertIn('trade_count', bid)
            self.assertIn('first_timestamp', bid)
            self.assertIn('last_timestamp', bid)

    def test_compute_lod3_single_price(self):
        """Test LOD3 computation with only one price level."""
        single_price_ticks = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'AAPL', 'price': 15000, 'volume': 200},
        ]
        result = self.engine.compute_lod3(single_price_ticks)

        self.assertEqual(result['symbol'], 'AAPL')
        self.assertEqual(result['mid_price'], 15000)
        self.assertEqual(result['spread'], 0)
        self.assertEqual(len(result['bids']), 0)
        self.assertEqual(len(result['asks']), 0)

    def test_compute_lod3_two_prices(self):
        """Test LOD3 computation with exactly two price levels."""
        two_price_ticks = [
            {'timestamp': 1000, 'symbol': 'AAPL', 'price': 15000, 'volume': 100},
            {'timestamp': 2000, 'symbol': 'AAPL', 'price': 15100, 'volume': 200},
        ]
        result = self.engine.compute_lod3(two_price_ticks)

        self.assertEqual(result['mid_price'], 15000)  # Median of [15000, 15100]
        self.assertEqual(len(result['bids']), 1)  # 15000
        self.assertEqual(len(result['asks']), 1)  # 15100
        self.assertEqual(result['spread'], 100)  # 15100 - 15000

    def test_stream_lod1(self):
        """Test streaming LOD1 price levels."""
        levels = list(self.engine.stream_lod1(self.sample_ticks))
        self.assertEqual(len(levels), 4)

        # Check first level
        first_level = levels[0]
        self.assertIn('price', first_level)
        self.assertIn('total_volume', first_level)
        self.assertIn('trade_count', first_level)

        # Prices should be sorted
        prices = [level['price'] for level in levels]
        self.assertEqual(prices, sorted(prices))

    def test_stream_lod2(self):
        """Test streaming LOD2 price levels."""
        levels = list(self.engine.stream_lod2(self.sample_ticks))
        self.assertEqual(len(levels), 4)

        # Check structure
        for level in levels:
            self.assertIn('price', level)
            self.assertIn('volume_buckets', level)
            self.assertEqual(len(level['volume_buckets']), 5)

    def test_stream_lod3_bids(self):
        """Test streaming LOD3 bid levels."""
        bids = list(self.engine.stream_lod3_bids(self.sample_ticks))
        self.assertEqual(len(bids), 2)

        # Should be sorted descending
        prices = [bid['price'] for bid in bids]
        self.assertEqual(prices, sorted(prices, reverse=True))

    def test_stream_lod3_asks(self):
        """Test streaming LOD3 ask levels."""
        asks = list(self.engine.stream_lod3_asks(self.sample_ticks))
        self.assertEqual(len(asks), 2)

        # Should be sorted ascending
        prices = [ask['price'] for ask in asks]
        self.assertEqual(prices, sorted(prices))

    def test_deterministic_processing(self):
        """Test that processing is deterministic."""
        result1 = self.engine.compute_lod1(self.sample_ticks)
        result2 = self.engine.compute_lod1(self.sample_ticks)

        self.assertEqual(result1, result2)

        # Also test with shuffled ticks
        import random
        shuffled_ticks = self.sample_ticks.copy()
        random.shuffle(shuffled_ticks)  # This is OK in test, not in production

        result3 = self.engine.compute_lod1(shuffled_ticks)
        # LOD1 should be the same regardless of order
        self.assertEqual(result1['total_volume'], result3['total_volume'])
        self.assertEqual(len(result1['price_levels']), len(result3['price_levels']))

    def test_only_int_values(self):
        """Test that all numeric values are integers."""
        result = self.engine.compute_lod1(self.sample_ticks)

        # Check top-level values
        self.assertIsInstance(result['total_volume'], int)
        self.assertIsInstance(result['total_trades'], int)
        self.assertIsInstance(result['timestamp_range'][0], int)
        self.assertIsInstance(result['timestamp_range'][1], int)

        # Check price levels
        for level in result['price_levels']:
            self.assertIsInstance(level['price'], int)
            self.assertIsInstance(level['total_volume'], int)
            self.assertIsInstance(level['trade_count'], int)
            self.assertIsInstance(level['first_timestamp'], int)
            self.assertIsInstance(level['last_timestamp'], int)

    def test_large_dataset(self):
        """Test with larger dataset to ensure no memory issues."""
        # Create 1000 ticks with predictable pattern
        large_ticks = []
        for i in range(1000):
            large_ticks.append({
                'timestamp': 1000 + i * 10,
                'symbol': 'TEST',
                'price': 10000 + (i % 10) * 100,  # 10 different prices
                'volume': (i % 5 + 1) * 100
            })

        # Should process without error
        result = self.engine.compute_lod1(large_ticks)
        self.assertEqual(result['total_trades'], 1000)
        self.assertEqual(len(result['price_levels']), 10)

        # Test streaming
        levels = list(self.engine.stream_lod1(large_ticks))
        self.assertEqual(len(levels), 10)


if __name__ == '__main__':
    unittest.main()
