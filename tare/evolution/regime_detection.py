"""
TARE regime_detection module - Identify and classify market regimes.
Phase 9: Deterministic regime detection using tick-level data.
"""

from typing import Optional


class RegimeDetector:
    """
    Identify and classify market regimes (trending, mean-revert, etc.).
    
    Uses deterministic algorithmic analysis of tick data to detect current
    market regime. All calculations use integer arithmetic only.
    """
    
    # Regime constants
    REGIME_TRENDING = "trending"
    REGIME_MEAN_REVERT = "mean_revert"
    REGIME_SIDEWAYS = "sideways"
    REGIME_VOLATILE = "volatile"
    REGIME_UNKNOWN = "unknown"
    
    def __init__(self, tick_data_engine):
        """
        Initialize RegimeDetector with tick data engine dependency.
        
        Args:
            tick_data_engine: TickDataEngine instance for accessing tick data.
        """
        if tick_data_engine is None:
            raise ValueError("tick_data_engine cannot be None")
        
        self._tick_engine = tick_data_engine
        self._regime_cache = {}
        self._regime_probabilities = {}
    
    def detect_regime(self, ticks: list[dict], lookback: int) -> str:
        """
        Identify and classify market regime based on recent tick data.
        
        Analyzes price action, volatility, and momentum over lookback period
        to determine current regime: trending, mean-revert, sideways, or volatile.
        Uses deterministic integer-only calculations.
        
        Args:
            ticks: List of tick dictionaries with timestamp, price, volume.
            lookback: Number of ticks to analyze (must be positive int).
            
        Returns:
            Regime classification string: 'trending', 'mean_revert', 'sideways',
            'volatile', or 'unknown' if insufficient data.
            
        Raises:
            ValueError: If ticks is not a list or lookback is not positive int.
        """
        if not isinstance(ticks, list):
            raise ValueError("ticks must be a list")
        
        if not isinstance(lookback, int) or lookback <= 0:
            raise ValueError("lookback must be a positive int")
        
        if len(ticks) < lookback:
            self._regime_cache['last'] = self.REGIME_UNKNOWN
            self._regime_probabilities['last'] = {
                self.REGIME_TRENDING: 0,
                self.REGIME_MEAN_REVERT: 0,
                self.REGIME_SIDEWAYS: 0,
                self.REGIME_VOLATILE: 0,
                self.REGIME_UNKNOWN: 10000
            }
            return self.REGIME_UNKNOWN
        
        # Extract lookback window
        window = ticks[-lookback:]
        
        # Validate tick structure
        for tick in window:
            if not isinstance(tick, dict) or 'price' not in tick:
                raise ValueError("Each tick must be a dict with 'price' key")
        
        # Calculate regime indicators (all integer)
        prices = [tick['price'] for tick in window]
        
        # Calculate trend strength using differences
        diffs = []
        for i in range(1, len(prices)):
            diffs.append(prices[i] - prices[i-1])
        
        if not diffs:
            self._regime_cache['last'] = self.REGIME_UNKNOWN
            return self.REGIME_UNKNOWN
        
        # Momentum: sum of price changes
        momentum = sum(diffs)
        
        # Volatility: sum of absolute changes
        volatility = sum(abs(d) for d in diffs)
        
        # Mean reversion: count reversals
        reversals = 0
        for i in range(1, len(diffs)):
            if (diffs[i] > 0 and diffs[i-1] < 0) or (diffs[i] < 0 and diffs[i-1] > 0):
                reversals += 1
        
        # High-low range
        price_min = min(prices)
        price_max = max(prices)
        price_range = price_max - price_min
        
        # Normalize metrics
        lookback_int = lookback
        momentum_abs = abs(momentum)
        
        # Determine regime using deterministic rules
        regime = self._classify_regime(
            momentum_abs,
            volatility,
            reversals,
            price_range,
            lookback_int
        )
        
        # Calculate probabilities for this regime
        probabilities = self._calculate_probabilities(
            momentum_abs,
            volatility,
            reversals,
            price_range,
            lookback_int
        )
        
        self._regime_cache['last'] = regime
        self._regime_probabilities['last'] = probabilities
        
        return regime
    
    def _classify_regime(
        self,
        momentum_abs: int,
        volatility: int,
        reversals: int,
        price_range: int,
        lookback: int
    ) -> str:
        """
        Classify regime based on calculated indicators.
        
        Args:
            momentum_abs: Absolute value of accumulated momentum.
            volatility: Sum of absolute price changes.
            reversals: Count of trend reversals.
            price_range: High-low price range.
            lookback: Lookback period.
            
        Returns:
            Regime classification string.
        """
        if volatility == 0:
            return self.REGIME_UNKNOWN
        
        # Normalize volatility to per-tick basis
        volatility_per_tick = volatility // lookback if lookback > 0 else 0
        momentum_per_tick = momentum_abs // lookback if lookback > 0 else 0
        
        # Reversal ratio
        reversal_ratio = (reversals * 10000) // lookback if lookback > 0 else 0
        
        # High volatility check
        if volatility_per_tick > (price_range // 2) if price_range > 0 else False:
            return self.REGIME_VOLATILE
        
        # High reversal check
        if reversal_ratio > 4000:  # >40% reversals
            return self.REGIME_MEAN_REVERT
        
        # Strong momentum check
        if momentum_per_tick > volatility_per_tick // 2:
            return self.REGIME_TRENDING
        
        # Default to sideways
        return self.REGIME_SIDEWAYS
    
    def _calculate_probabilities(
        self,
        momentum_abs: int,
        volatility: int,
        reversals: int,
        price_range: int,
        lookback: int
    ) -> dict[str, int]:
        """
        Calculate regime probabilities in basis points (0-10000).
        
        Args:
            momentum_abs: Absolute momentum value.
            volatility: Volatility measure.
            reversals: Reversal count.
            price_range: Price range.
            lookback: Lookback period.
            
        Returns:
            Dict mapping regime names to probability basis points.
        """
        if volatility == 0:
            return {
                self.REGIME_TRENDING: 0,
                self.REGIME_MEAN_REVERT: 0,
                self.REGIME_SIDEWAYS: 0,
                self.REGIME_VOLATILE: 0,
                self.REGIME_UNKNOWN: 10000
            }
        
        # Calculate base probabilities
        volatility_per_tick = volatility // lookback if lookback > 0 else 0
        momentum_per_tick = momentum_abs // lookback if lookback > 0 else 0
        reversal_ratio = (reversals * 10000) // lookback if lookback > 0 else 0
        
        # Trending probability: momentum strength
        trending_prob = min(
            10000,
            (momentum_per_tick * 10000) // (volatility_per_tick + 1)
        )
        
        # Mean revert probability: high reversal rate
        mean_revert_prob = min(10000, reversal_ratio // 2)
        
        # Volatile probability: high per-tick volatility
        volatile_prob = min(
            10000,
            (volatility_per_tick * 10000) // (price_range + 1)
        )
        
        # Sideways probability: low momentum and reversals
        sideways_prob = max(
            0,
            10000 - trending_prob - mean_revert_prob - volatile_prob
        )
        
        # Normalize to sum to 10000
        total = trending_prob + mean_revert_prob + volatile_prob + sideways_prob
        if total == 0:
            total = 1
        
        scaling = 10000 // total if total > 0 else 1
        
        trending_prob = (trending_prob * scaling) // 10000
        mean_revert_prob = (mean_revert_prob * scaling) // 10000
        volatile_prob = (volatile_prob * scaling) // 10000
        sideways_prob = (sideways_prob * scaling) // 10000
        
        # Adjust for rounding
        remainder = 10000 - (trending_prob + mean_revert_prob + volatile_prob + sideways_prob)
        sideways_prob += remainder
        
        return {
            self.REGIME_TRENDING: max(0, trending_prob),
            self.REGIME_MEAN_REVERT: max(0, mean_revert_prob),
            self.REGIME_SIDEWAYS: max(0, sideways_prob),
            self.REGIME_VOLATILE: max(0, volatile_prob),
            self.REGIME_UNKNOWN: 0
        }
    
    def get_regime_probability(self, regime: str) -> int:
        """
        Retrieve probability of specified regime in basis points.
        
        Returns probability of given regime from most recent detect_regime() call.
        Probability is in basis points (0-10000), where 10000 = 100% certainty.
        
        Args:
            regime: Regime name string (e.g., 'trending', 'mean_revert').
            
        Returns:
            Probability in basis points (0-10000). Returns 0 if regime unknown
            or detect_regime() not yet called.
            
        Raises:
            ValueError: If regime is not a string.
        """
        if not isinstance(regime, str):
            raise ValueError("regime must be a string")
        
        probs = self._regime_probabilities.get('last', {})
        
        if regime not in probs:
            return 0
        
        return probs[regime]
    
    def get_all_probabilities(self) -> dict[str, int]:
        """
        Retrieve all regime probabilities from last detection.
        
        Returns:
            Dict mapping regime names to probability basis points (0-10000).
            Returns empty dict if detect_regime() not yet called.
        """
        return self._regime_probabilities.get('last', {}).copy()
    
    def get_regime_history(self, ticks: list[dict], window_size: int, step: int) -> list[tuple[int, str]]:
        """
        Generate regime classification history using sliding window.
        
        Applies regime detection across multiple overlapping or non-overlapping
        windows of tick data, returning regime sequence with timestamps.
        
        Args:
            ticks: List of tick dictionaries.
            window_size: Size of each analysis window (positive int).
            step: Number of ticks to advance between windows (positive int).
            
        Yields:
            Tuples of (timestamp, regime_string) for each window.
            
        Raises:
            ValueError: If parameters are invalid.
        """
        if not isinstance(ticks, list):
            raise ValueError("ticks must be a list")
        
        if not isinstance(window_size, int) or window_size <= 0:
            raise ValueError("window_size must be positive int")
        
        if not isinstance(step, int) or step <= 0:
            raise ValueError("step must be positive int")
        
        history = []
        
        pos = 0
        while pos + window_size <= len(ticks):
            window = ticks[pos:pos + window_size]
            
            if window and 'timestamp' in window[-1]:
                timestamp = window[-1]['timestamp']
                regime = self.detect_regime(window, window_size)
                history.append((timestamp, regime))
            
            pos += step
        
        return history
    
    def analyze_regime_duration(self, ticks: list[dict], window_size: int) -> dict[str, int]:
        """
        Analyze duration and frequency of regime occurrences.
        
        Slides window across ticks and counts how many times each regime appears.
        Returns counts in basis points relative to total windows analyzed.
        
        Args:
            ticks: List of tick dictionaries.
            window_size: Analysis window size (positive int).
            
        Returns:
            Dict mapping regime names to occurrence counts (integers).
            
        Raises:
            ValueError: If parameters invalid.
        """
        if not isinstance(ticks, list):
            raise ValueError("ticks must be a list")
        
        if not isinstance(window_size, int) or window_size <= 0:
            raise ValueError("window_size must be positive int")
        
        regime_counts = {
            self.REGIME_TRENDING: 0,
            self.REGIME_MEAN_REVERT: 0,
            self.REGIME_SIDEWAYS: 0,
            self.REGIME_VOLATILE: 0,
            self.REGIME_UNKNOWN: 0
        }
        
        total_windows = 0
        
        for i in range(len(ticks) - window_size + 1):
            window = ticks[i:i + window_size]
            regime = self.detect_regime(window, window_size)
            
            if regime in regime_counts:
                regime_counts[regime] += 1
            
            total_windows += 1
        
        return regime_counts
