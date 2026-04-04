"""
Level-of-detail (LOD) modeling engine for market depth.

Transforms tick-level data into aggregated market depth views at different
levels of detail (LOD1, LOD2, LOD3) for microstructure analysis.
"""

from typing import Generator
from collections import defaultdict


class LODEngine:
    """
    Engine for computing Level-of-Detail (LOD) models from tick data.

    LOD1: Price-time aggregated view (price level aggregation)
    LOD2: Volume-time aggregated view (volume buckets per price)
    LOD3: Full order book reconstruction (bid/ask sides with price levels)
    """

    def __init__(self):
        """Initialize LOD engine with deterministic processing."""
        pass

    def compute_lod1(self, ticks: list[dict]) -> dict:
        """
        Compute LOD1: Price-time aggregated view.

        Aggregates ticks by price level, showing total volume traded at each price.
        This is the most basic market depth view showing price distribution.

        Args:
            ticks: List of tick dictionaries with fields:
                   - timestamp (int): Nanoseconds since epoch
                   - symbol (str): Asset symbol
                   - price (int): Price in base units
                   - volume (int): Volume in units

        Returns:
            Dictionary with LOD1 structure:
            {
                'symbol': str,
                'price_levels': [
                    {
                        'price': int,
                        'total_volume': int,
                        'trade_count': int,
                        'first_timestamp': int,
                        'last_timestamp': int
                    },
                    ...
                ],
                'total_volume': int,
                'total_trades': int,
                'timestamp_range': (int, int)
            }

        Raises:
            ValueError: If ticks list is empty or missing required fields.
        """
        if not ticks:
            raise ValueError("ticks list cannot be empty")

        # Validate tick structure
        required_fields = {'timestamp', 'symbol', 'price', 'volume'}
        for i, tick in enumerate(ticks):
            if not required_fields.issubset(tick.keys()):
                raise ValueError(f"Tick at index {i} missing required fields")

        # Group ticks by price
        price_groups = defaultdict(list)
        min_ts = None
        max_ts = None
        total_volume = 0

        for tick in ticks:
            price = tick['price']
            price_groups[price].append(tick)

            ts = tick['timestamp']
            if min_ts is None or ts < min_ts:
                min_ts = ts
            if max_ts is None or ts > max_ts:
                max_ts = ts

            total_volume += tick['volume']

        # Build price levels
        price_levels = []
        for price in sorted(price_groups.keys()):
            group = price_groups[price]
            group_volume = sum(tick['volume'] for tick in group)
            group_timestamps = [tick['timestamp'] for tick in group]

            price_levels.append({
                'price': price,
                'total_volume': group_volume,
                'trade_count': len(group),
                'first_timestamp': min(group_timestamps),
                'last_timestamp': max(group_timestamps)
            })

        return {
            'symbol': ticks[0]['symbol'],
            'price_levels': price_levels,
            'total_volume': total_volume,
            'total_trades': len(ticks),
            'timestamp_range': (min_ts, max_ts)
        }

    def compute_lod2(self, ticks: list[dict]) -> dict:
        """
        Compute LOD2: Volume-time aggregated view.

        Aggregates ticks into volume buckets per price level, showing
        volume distribution over time within each price level.

        Args:
            ticks: List of tick dictionaries with same structure as LOD1.

        Returns:
            Dictionary with LOD2 structure:
            {
                'symbol': str,
                'price_levels': [
                    {
                        'price': int,
                        'volume_buckets': [
                            {
                                'bucket_start': int,
                                'bucket_end': int,
                                'volume': int,
                                'trade_count': int
                            },
                            ...
                        ],
                        'total_volume': int,
                        'total_trades': int
                    },
                    ...
                ],
                'total_volume': int,
                'total_trades': int,
                'timestamp_range': (int, int)
            }

        Raises:
            ValueError: If ticks list is empty or missing required fields.
        """
        if not ticks:
            raise ValueError("ticks list cannot be empty")

        # Validate tick structure
        required_fields = {'timestamp', 'symbol', 'price', 'volume'}
        for i, tick in enumerate(ticks):
            if not required_fields.issubset(tick.keys()):
                raise ValueError(f"Tick at index {i} missing required fields")

        # First compute LOD1 to get price levels
        lod1 = self.compute_lod1(ticks)
        min_ts, max_ts = lod1['timestamp_range']

        # Group ticks by price
        price_groups = defaultdict(list)
        for tick in ticks:
            price_groups[tick['price']].append(tick)

        # Build volume buckets for each price level
        price_levels = []
        for price_level in lod1['price_levels']:
            price = price_level['price']
            group = price_groups[price]

            # Sort ticks by timestamp
            sorted_ticks = sorted(group, key=lambda x: x['timestamp'])

            # Create volume buckets (5 buckets per price level)
            bucket_count = 5
            time_range = price_level['last_timestamp'] - price_level['first_timestamp']
            bucket_duration = time_range // bucket_count if time_range > 0 else 1

            buckets = []
            for i in range(bucket_count):
                bucket_start = price_level['first_timestamp'] + (i * bucket_duration)
                bucket_end = bucket_start + bucket_duration - 1 if i < bucket_count - 1 else price_level['last_timestamp']

                # Aggregate ticks in this bucket
                bucket_volume = 0
                bucket_trades = 0

                for tick in sorted_ticks:
                    if bucket_start <= tick['timestamp'] <= bucket_end:
                        bucket_volume += tick['volume']
                        bucket_trades += 1

                buckets.append({
                    'bucket_start': bucket_start,
                    'bucket_end': bucket_end,
                    'volume': bucket_volume,
                    'trade_count': bucket_trades
                })

            price_levels.append({
                'price': price,
                'volume_buckets': buckets,
                'total_volume': price_level['total_volume'],
                'total_trades': price_level['trade_count']
            })

        return {
            'symbol': lod1['symbol'],
            'price_levels': price_levels,
            'total_volume': lod1['total_volume'],
            'total_trades': lod1['total_trades'],
            'timestamp_range': (min_ts, max_ts)
        }

    def compute_lod3(self, ticks: list[dict]) -> dict:
        """
        Compute LOD3: Full order book reconstruction.

        Reconstructs bid and ask sides with price levels, simulating
        an order book from tick data. Assumes each tick represents
        a market order that consumes liquidity.

        Args:
            ticks: List of tick dictionaries with same structure as LOD1.

        Returns:
            Dictionary with LOD3 structure:
            {
                'symbol': str,
                'bids': [
                    {
                        'price': int,
                        'total_volume': int,
                        'remaining_volume': int,
                        'trade_count': int,
                        'first_timestamp': int,
                        'last_timestamp': int
                    },
                    ...
                ],
                'asks': [
                    {
                        'price': int,
                        'total_volume': int,
                        'remaining_volume': int,
                        'trade_count': int,
                        'first_timestamp': int,
                        'last_timestamp': int
                    },
                    ...
                ],
                'mid_price': int,
                'spread': int,
                'total_volume': int,
                'total_trades': int,
                'timestamp_range': (int, int)
            }

        Raises:
            ValueError: If ticks list is empty or missing required fields.
        """
        if not ticks:
            raise ValueError("ticks list cannot be empty")

        # Validate tick structure
        required_fields = {'timestamp', 'symbol', 'price', 'volume'}
        for i, tick in enumerate(ticks):
            if not required_fields.issubset(tick.keys()):
                raise ValueError(f"Tick at index {i} missing required fields")

        # First compute LOD1 to get price levels
        lod1 = self.compute_lod1(ticks)
        min_ts, max_ts = lod1['timestamp_range']

        # Sort price levels
        sorted_prices = sorted([level['price'] for level in lod1['price_levels']])

        if len(sorted_prices) < 2:
            # Not enough price levels for bid/ask separation
            mid_price = sorted_prices[0] if sorted_prices else 0
            return {
                'symbol': lod1['symbol'],
                'bids': [],
                'asks': [],
                'mid_price': mid_price,
                'spread': 0,
                'total_volume': lod1['total_volume'],
                'total_trades': lod1['total_trades'],
                'timestamp_range': (min_ts, max_ts)
            }

        # Find median price as approximate mid price
        median_idx = (len(sorted_prices) - 1) // 2
        mid_price = sorted_prices[median_idx]

        # Separate bids (prices <= mid_price) and asks (prices > mid_price)
        bids = []
        asks = []

        for level in lod1['price_levels']:
            price = level['price']
            if price <= mid_price:
                # Bid side: higher prices are better
                bids.append({
                    'price': price,
                    'total_volume': level['total_volume'],
                    'remaining_volume': level['total_volume'],  # Initially all volume available
                    'trade_count': level['trade_count'],
                    'first_timestamp': level['first_timestamp'],
                    'last_timestamp': level['last_timestamp']
                })
            else:
                # Ask side: lower prices are better
                asks.append({
                    'price': price,
                    'total_volume': level['total_volume'],
                    'remaining_volume': level['total_volume'],  # Initially all volume available
                    'trade_count': level['trade_count'],
                    'first_timestamp': level['first_timestamp'],
                    'last_timestamp': level['last_timestamp']
                })

        # Sort bids descending (best bid first) and asks ascending (best ask first)
        bids.sort(key=lambda x: x['price'], reverse=True)
        asks.sort(key=lambda x: x['price'])

        # Calculate spread
        spread = 0
        if bids and asks:
            best_bid = bids[0]['price']
            best_ask = asks[0]['price']
            spread = best_ask - best_bid

        return {
            'symbol': lod1['symbol'],
            'bids': bids,
            'asks': asks,
            'mid_price': mid_price,
            'spread': spread,
            'total_volume': lod1['total_volume'],
            'total_trades': lod1['total_trades'],
            'timestamp_range': (min_ts, max_ts)
        }

    def stream_lod1(self, ticks: list[dict]) -> Generator[dict, None, None]:
        """
        Stream LOD1 price levels one at a time.

        Args:
            ticks: List of tick dictionaries.

        Yields:
            Individual price level dictionaries from LOD1.

        Raises:
            ValueError: If ticks list is empty.
        """
        lod1 = self.compute_lod1(ticks)
        for level in lod1['price_levels']:
            yield level

    def stream_lod2(self, ticks: list[dict]) -> Generator[dict, None, None]:
        """
        Stream LOD2 price levels with volume buckets.

        Args:
            ticks: List of tick dictionaries.

        Yields:
            Individual price level dictionaries from LOD2.

        Raises:
            ValueError: If ticks list is empty.
        """
        lod2 = self.compute_lod2(ticks)
        for level in lod2['price_levels']:
            yield level

    def stream_lod3_bids(self, ticks: list[dict]) -> Generator[dict, None, None]:
        """
        Stream LOD3 bid levels.

        Args:
            ticks: List of tick dictionaries.

        Yields:
            Individual bid level dictionaries from LOD3.

        Raises:
            ValueError: If ticks list is empty.
        """
        lod3 = self.compute_lod3(ticks)
        for bid in lod3['bids']:
            yield bid

    def stream_lod3_asks(self, ticks: list[dict]) -> Generator[dict, None, None]:
        """
        Stream LOD3 ask levels.

        Args:
            ticks: List of tick dictionaries.

        Yields:
            Individual ask level dictionaries from LOD3.

        Raises:
            ValueError: If ticks list is empty.
        """
        lod3 = self.compute_lod3(ticks)
        for ask in lod3['asks']:
            yield ask
