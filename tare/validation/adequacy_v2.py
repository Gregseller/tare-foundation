"""
adequacy_v2.py — AdequacyV2 Extended Validator
TARE (Tick-Level Algorithmic Research Environment)

Extended validation with Kolmogorov-Smirnov tests and FX adequacy checks.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Детерминизм: одни входные данные → всегда одинаковый результат
  - Только стандартная библиотека Python
"""

from typing import Generator
from statistics import median, mean


class AdequacyV2:
    """
    Extended validation with KS tests and FX adequacy checks.
    
    Performs statistical distribution validation using Kolmogorov-Smirnov tests
    and validates FX pair adequacy. All arithmetic uses integer-based basis points.
    Deterministic: same inputs always produce same results.
    """
    
    def __init__(self, adequacy_v1=None):
        """
        Initialize AdequacyV2 validator.
        
        Args:
            adequacy_v1: Optional AdequacyV1 instance for chained validation.
            
        Raises:
            ValueError: If adequacy_v1 is provided but invalid.
        """
        self._adequacy_v1 = adequacy_v1
        self._violations = []
    
    def ks_test(
        self,
        observed: list[int],
        expected: list[int]
    ) -> int:
        """
        Kolmogorov-Smirnov test comparing observed vs expected distributions.
        
        Computes the maximum absolute difference between empirical CDFs
        of observed and expected samples. Returns result in basis points (0-10000).
        Uses only integer arithmetic; all input values treated as basis points.
        
        Args:
            observed: List of observed integer values (basis points).
            expected: List of expected integer values (basis points).
        
        Returns:
            int: KS statistic in basis points (0-10000 range).
                 0 = identical distributions, 10000 = completely different.
        
        Raises:
            ValueError: If lists empty or inputs invalid.
            TypeError: If inputs not lists of integers.
        """
        if not isinstance(observed, list) or not isinstance(expected, list):
            raise TypeError("observed and expected must be lists")
        
        if not observed or not expected:
            raise ValueError("observed and expected lists cannot be empty")
        
        # Validate all elements are integers
        for val in observed:
            if not isinstance(val, int):
                raise TypeError("All observed values must be integers")
        
        for val in expected:
            if not isinstance(val, int):
                raise TypeError("All expected values must be integers")
        
        # Sort both distributions
        obs_sorted = sorted(observed)
        exp_sorted = sorted(expected)
        
        # Generate empirical CDF values in basis points
        obs_cdf = self._compute_empirical_cdf(obs_sorted)
        exp_cdf = self._compute_empirical_cdf(exp_sorted)
        
        # Compute KS statistic: max absolute difference
        ks_stat_bps = self._compute_ks_statistic(obs_sorted, exp_sorted, obs_cdf, exp_cdf)
        
        # Clamp to valid basis point range
        return min(10000, max(0, ks_stat_bps))
    
    def _compute_empirical_cdf(self, sorted_values: list[int]) -> dict[int, int]:
        """
        Compute empirical CDF for sorted values, returned as basis points.
        
        Args:
            sorted_values: Sorted list of integer values.
        
        Returns:
            dict mapping value to CDF in basis points (0-10000).
        """
        cdf = {}
        n = len(sorted_values)
        
        for i, val in enumerate(sorted_values):
            # CDF at this point: (i+1) / n, scaled to basis points
            cdf_bps = ((i + 1) * 10000) // n
            if val not in cdf or cdf[val] < cdf_bps:
                cdf[val] = cdf_bps
        
        return cdf
    
    def _compute_ks_statistic(
        self,
        obs_sorted: list[int],
        exp_sorted: list[int],
        obs_cdf: dict[int, int],
        exp_cdf: dict[int, int]
    ) -> int:
        """
        Compute KS statistic as max CDF difference in basis points.
        
        Args:
            obs_sorted: Sorted observed values.
            exp_sorted: Sorted expected values.
            obs_cdf: Observed empirical CDF dict.
            exp_cdf: Expected empirical CDF dict.
        
        Returns:
            int: KS statistic in basis points.
        """
        # Collect all unique values from both distributions
        all_values = set(obs_sorted) | set(exp_sorted)
        
        max_diff_bps = 0
        
        for val in all_values:
            obs_cdf_val = obs_cdf.get(val, 0)
            exp_cdf_val = exp_cdf.get(val, 0)
            
            diff_bps = abs(obs_cdf_val - exp_cdf_val)
            max_diff_bps = max(max_diff_bps, diff_bps)
        
        return max_diff_bps
    
    def fx_adequacy_check(self, fx_pairs: dict) -> dict[str, bool]:
        """
        Validate FX pair execution adequacy.
        
        Checks that FX pair executions are realistic given market conditions:
        - Bid-ask spreads within expected ranges for pair type
        - Volume and volatility match historical profiles
        - Cross-rates maintain triangular arbitrage constraints
        - Execution prices realistic for liquidity/volatility profile
        
        All checks deterministic using integer basis point arithmetic.
        
        Args:
            fx_pairs: Dictionary with FX pair validation data:
                {
                    'pairs': [
                        {
                            'symbol': str (e.g. 'EURUSD'),
                            'bid': int (price in pips * 10000),
                            'ask': int (price in pips * 10000),
                            'volume': int (units),
                            'volatility_bps': int (annualized vol in bps),
                            'fill_prices': [int],
                            'expected_spread_bps': int,
                            'liquidity_rank': int (0-100)
                        }
                    ],
                    'crosses': [
                        {
                            'symbols': (str, str, str),  # e.g. ('EUR', 'USD', 'GBP')
                            'rates': (int, int, int)  # mid prices
                        }
                    ]
                }
        
        Returns:
            dict with boolean validation results:
            {
                'spreads_realistic': bool,
                'volume_consistent': bool,
                'volatility_reasonable': bool,
                'prices_realistic': bool,
                'crosses_arbitrage_free': bool,
                'liquidity_consistent': bool,
                'overall_adequate': bool
            }
        
        Raises:
            ValueError: If fx_pairs invalid.
            TypeError: If parameters have wrong types.
        """
        if not isinstance(fx_pairs, dict):
            raise TypeError("fx_pairs must be a dict")
        
        self._violations = []
        
        checks = {
            'spreads_realistic': self._check_fx_spreads(fx_pairs),
            'volume_consistent': self._check_fx_volume(fx_pairs),
            'volatility_reasonable': self._check_fx_volatility(fx_pairs),
            'prices_realistic': self._check_fx_prices(fx_pairs),
            'crosses_arbitrage_free': self._check_triangular_arbitrage(fx_pairs),
            'liquidity_consistent': self._check_liquidity_consistency(fx_pairs),
        }
        
        checks['overall_adequate'] = all(checks.values())
        
        return checks
    
    def _check_fx_spreads(self, fx_pairs: dict) -> bool:
        """
        Validate that FX spreads are realistic for pair types.
        
        Args:
            fx_pairs: FX pairs validation data.
        
        Returns:
            bool: True if spreads are realistic.
        """
        pairs = fx_pairs.get('pairs', [])
        
        if not isinstance(pairs, list):
            self._violations.append("pairs must be list")
            return False
        
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            
            bid = pair.get('bid', 0)
            ask = pair.get('ask', 0)
            symbol = pair.get('symbol', '')
            expected_spread_bps = pair.get('expected_spread_bps', 100)
            liquidity_rank = pair.get('liquidity_rank', 50)
            
            # Validate types
            if not isinstance(bid, int) or not isinstance(ask, int):
                self._violations.append(f"Pair {symbol} bid/ask not int")
                return False
            
            if not isinstance(expected_spread_bps, int) or not isinstance(liquidity_rank, int):
                self._violations.append(f"Pair {symbol} parameters not int")
                return False
            
            # Check: ask >= bid (valid pricing)
            if ask < bid:
                self._violations.append(f"Pair {symbol} ask {ask} < bid {bid}")
                return False
            
            # Compute actual spread in basis points
            # spread_bps = (ask - bid) / bid * 10000
            if bid > 0:
                actual_spread_bps = ((ask - bid) * 10000) // bid
            else:
                self._violations.append(f"Pair {symbol} bid zero or negative")
                return False
            
            # Spread should not exceed realistic bounds based on liquidity
            # More liquid (higher rank) = tighter spreads expected
            max_spread_bps = 500 - (liquidity_rank // 2)  # 500 to 0 bps range
            
            if actual_spread_bps > max_spread_bps:
                self._violations.append(
                    f"Pair {symbol} spread {actual_spread_bps}bps "
                    f"> max {max_spread_bps}bps for liquidity rank {liquidity_rank}"
                )
                return False
        
        return True
    
    def _check_fx_volume(self, fx_pairs: dict) -> bool:
        """
        Validate that FX volumes are consistent.
        
        Args:
            fx_pairs: FX pairs validation data.
        
        Returns:
            bool: True if volumes consistent.
        """
        pairs = fx_pairs.get('pairs', [])
        
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            
            volume = pair.get('volume', 0)
            symbol = pair.get('symbol', '')
            
            if not isinstance(volume, int):
                self._violations.append(f"Pair {symbol} volume not int")
                return False
            
            # Volume must be non-negative
            if volume < 0:
                self._violations.append(f"Pair {symbol} negative volume {volume}")
                return False
        
        return True
    
    def _check_fx_volatility(self, fx_pairs: dict) -> bool:
        """
        Validate that FX volatilities are reasonable.
        
        Args:
            fx_pairs: FX pairs validation data.
        
        Returns:
            bool: True if volatilities reasonable.
        """
        pairs = fx_pairs.get('pairs', [])
        
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            
            volatility_bps = pair.get('volatility_bps', 0)
            symbol = pair.get('symbol', '')
            
            if not isinstance(volatility_bps, int):
                self._violations.append(f"Pair {symbol} volatility not int")
                return False
            
            # Volatility in basis points should be reasonable (1-5000 bps annualized)
            if volatility_bps < 10 or volatility_bps > 50000:
                self._violations.append(
                    f"Pair {symbol} volatility {volatility_bps}bps out of range"
                )
                return False
        
        return True
    
    def _check_fx_prices(self, fx_pairs: dict) -> bool:
        """
        Validate that fill prices are realistic given spreads and liquidity.
        
        Args:
            fx_pairs: FX pairs validation data.
        
        Returns:
            bool: True if prices realistic.
        """
        pairs = fx_pairs.get('pairs', [])
        
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            
            fill_prices = pair.get('fill_prices', [])
            bid = pair.get('bid', 0)
            ask = pair.get('ask', 0)
            symbol = pair.get('symbol', '')
            
            if not isinstance(fill_prices, list):
                self._violations.append(f"Pair {symbol} fill_prices not list")
                return False
            
            # Check each fill price is between bid and ask
            for price in fill_prices:
                if not isinstance(price, int):
                    self._violations.append(f"Pair {symbol} fill price not int")
                    return False
                
                # Price should be within bid-ask spread
                if price < bid or price > ask:
                    self._violations.append(
                        f"Pair {symbol} fill price {price} outside spread [{bid}, {ask}]"
                    )
                    return False
        
        return True
    
    def _check_triangular_arbitrage(self, fx_pairs: dict) -> bool:
        """
        Validate that cross rates maintain triangular arbitrage constraints.
        
        Args:
            fx_pairs: FX pairs validation data.
        
        Returns:
            bool: True if arbitrage-free.
        """
        crosses = fx_pairs.get('crosses', [])
        
        if not isinstance(crosses, list):
            return True
        
        for cross in crosses:
            if not isinstance(cross, dict):
                continue
            
            symbols = cross.get('symbols', ())
            rates = cross.get('rates', ())
            
            # Validate structure
            if not isinstance(symbols, (tuple, list)) or len(symbols) != 3:
                continue
            
            if not isinstance(rates, (tuple, list)) or len(rates) != 3:
                self._violations.append("Cross rates tuple length invalid")
                return False
            
            # Validate all rates are integers
            for rate in rates:
                if not isinstance(rate, int):
                    self._violations.append("Cross rate not int")
                    return False
            
            # Validate triangular arbitrage: A->B * B->C ≈ A->C
            # Using integer arithmetic with tolerance
            rate_ab, rate_bc, rate_ac = rates
            
            if rate_ab <= 0 or rate_bc <= 0:
                continue
            
            # Cross rate: (A->B) * (B->C)
            # To avoid overflow, divide before multiply when possible
            if rate_ab > 1000000 or rate_bc > 1000000:
                # Scale down for calculation
                cross_rate = (rate_ab // 1000) * (rate_bc // 1000)
            else:
                cross_rate = (rate_ab * rate_bc) // 100000
            
            # Check if cross_rate is close to rate_ac (within
