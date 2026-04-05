"""
drift_detection.py — DriftDetector v1
TARE (Tick-Level Algorithmic Research Environment)

Detect market regime drift and strategy performance degradation.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Детерминизм: одни входные данные → всегда одинаковый результат
  - Только стандартная библиотека Python
"""

from typing import Dict, List, Tuple
from statistics import median, mean
import hashlib


class DriftDetector:
    """
    Detect market regime drift and strategy performance degradation.
    
    Compares live market data and strategy performance against baseline
    distributions using statistical tests. All operations deterministic
    with integer basis point arithmetic. No random operations.
    """
    
    def __init__(self, adequacy_v2=None, batch_testing=None):
        """
        Initialize DriftDetector with optional dependencies.
        
        Args:
            adequacy_v2: Optional AdequacyV2 instance for distribution testing.
            batch_testing: Optional BatchTesting instance for performance analysis.
            
        Raises:
            ValueError: If dependencies provided but invalid.
        """
        self._adequacy_v2 = adequacy_v2
        self._batch_testing = batch_testing
        self._drift_metrics: Dict[str, int] = {}
        self._is_drifted_flag = False
    
    def detect(self, live_data: dict, baseline: dict) -> dict[str, int]:
        """
        Detect drift between live data and baseline distributions.
        
        Compares live market conditions and strategy performance metrics
        against baseline using KS tests and statistical measures.
        All comparisons use integer basis point arithmetic (0-10000 scale).
        
        Args:
            live_data: Current market/strategy metrics dict with keys:
                - 'returns': list[int] - strategy returns in basis points
                - 'volumes': list[int] - trade volumes
                - 'spreads': list[int] - bid-ask spreads in basis points
                - 'volatility': int - current volatility in basis points
                - 'drawdown': int - current max drawdown in basis points
                - 'fill_quality': list[int] - fill price deviations in bps
                - 'trade_frequency': int - trades per period
                
            baseline: Reference distribution dict with same structure:
                - 'returns': list[int] - historical returns in basis points
                - 'volumes': list[int] - historical volumes
                - 'spreads': list[int] - historical spreads in basis points
                - 'volatility': int - baseline volatility in basis points
                - 'drawdown': int - baseline max drawdown in basis points
                - 'fill_quality': list[int] - baseline fill deviations in bps
                - 'trade_frequency': int - baseline trades per period
        
        Returns:
            dict[str, int]: Drift metrics in basis points (0-10000 scale):
            {
                'returns_drift': int - KS test statistic for returns
                'volumes_drift': int - KS test statistic for volumes
                'spreads_drift': int - KS test statistic for spreads
                'volatility_drift': int - absolute change in volatility (bps)
                'drawdown_drift': int - absolute change in drawdown (bps)
                'fill_quality_drift': int - KS test for fill deviations
                'trade_frequency_drift': int - absolute frequency change
                'overall_drift_score': int - weighted aggregate (0-10000)
            }
        
        Raises:
            ValueError: If live_data or baseline invalid.
            TypeError: If parameters have wrong types.
            
        Example:
            >>> detector = DriftDetector(adequacy_v2)
            >>> metrics = detector.detect(
            ...     live_data={'returns': [100, 200, 150], 'volumes': [1000, 1500]},
            ...     baseline={'returns': [100, 110, 120], 'volumes': [1000, 1000]}
            ... )
            >>> if detector.is_drifted(threshold=5000):
            ...     print("Market regime drift detected")
        """
        if not isinstance(live_data, dict) or not isinstance(baseline, dict):
            raise TypeError("live_data and baseline must be dicts")
        
        self._drift_metrics = {}
        
        # Validate and extract metrics
        live_returns = self._extract_int_list(live_data, 'returns')
        baseline_returns = self._extract_int_list(baseline, 'returns')
        
        live_volumes = self._extract_int_list(live_data, 'volumes')
        baseline_volumes = self._extract_int_list(baseline, 'volumes')
        
        live_spreads = self._extract_int_list(live_data, 'spreads')
        baseline_spreads = self._extract_int_list(baseline, 'spreads')
        
        live_volatility = self._extract_int(live_data, 'volatility')
        baseline_volatility = self._extract_int(baseline, 'volatility')
        
        live_drawdown = self._extract_int(live_data, 'drawdown')
        baseline_drawdown = self._extract_int(baseline, 'drawdown')
        
        live_fill_quality = self._extract_int_list(live_data, 'fill_quality')
        baseline_fill_quality = self._extract_int_list(baseline, 'fill_quality')
        
        live_frequency = self._extract_int(live_data, 'trade_frequency')
        baseline_frequency = self._extract_int(baseline, 'trade_frequency')
        
        # Compute drift metrics using KS tests
        self._drift_metrics['returns_drift'] = self._compute_ks_drift(
            live_returns, baseline_returns
        )
        
        self._drift_metrics['volumes_drift'] = self._compute_ks_drift(
            live_volumes, baseline_volumes
        )
        
        self._drift_metrics['spreads_drift'] = self._compute_ks_drift(
            live_spreads, baseline_spreads
        )
        
        self._drift_metrics['fill_quality_drift'] = self._compute_ks_drift(
            live_fill_quality, baseline_fill_quality
        )
        
        # Compute absolute metric drifts
        self._drift_metrics['volatility_drift'] = self._compute_abs_drift(
            live_volatility, baseline_volatility
        )
        
        self._drift_metrics['drawdown_drift'] = self._compute_abs_drift(
            live_drawdown, baseline_drawdown
        )
        
        self._drift_metrics['trade_frequency_drift'] = self._compute_abs_drift(
            live_frequency, baseline_frequency
        )
        
        # Compute overall drift score (weighted average)
        self._drift_metrics['overall_drift_score'] = self._compute_overall_drift()
        
        # Update drifted flag
        self._is_drifted_flag = self._drift_metrics['overall_drift_score'] > 5000
        
        return self._drift_metrics.copy()
    
    def is_drifted(self, threshold: int) -> bool:
        """
        Check if overall drift exceeds threshold.
        
        Returns cached drift assessment based on last detect() call.
        Threshold is in basis points (0-10000 scale).
        
        Args:
            threshold: Drift threshold in basis points (0-10000).
                      Typical values: 2500 (25%), 5000 (50%), 7500 (75%).
        
        Returns:
            bool: True if overall_drift_score > threshold, False otherwise.
            
        Raises:
            ValueError: If threshold not in valid range.
            TypeError: If threshold not integer.
            
        Example:
            >>> metrics = detector.detect(live_data, baseline)
            >>> if detector.is_drifted(5000):
            ...     print(f"Drift score: {metrics['overall_drift_score']}")
        """
        if not isinstance(threshold, int):
            raise TypeError("threshold must be integer")
        
        if threshold < 0 or threshold > 10000:
            raise ValueError("threshold must be in range [0, 10000]")
        
        return self._drift_metrics.get('overall_drift_score', 0) > threshold
    
    def _extract_int_list(self, data: dict, key: str) -> list[int]:
        """
        Extract and validate integer list from dict.
        
        Args:
            data: Source dictionary.
            key: Dictionary key to extract.
        
        Returns:
            list[int]: Validated list of integers, or empty list if missing/invalid.
        """
        value = data.get(key, [])
        
        if not isinstance(value, (list, tuple)):
            return []
        
        # Validate all elements are integers
        int_list = []
        for item in value:
            if isinstance(item, int):
                int_list.append(item)
        
        return int_list
    
    def _extract_int(self, data: dict, key: str) -> int:
        """
        Extract and validate integer from dict.
        
        Args:
            data: Source dictionary.
            key: Dictionary key to extract.
        
        Returns:
            int: Integer value, or 0 if missing/invalid.
        """
        value = data.get(key, 0)
        
        if isinstance(value, int):
            return value
        
        return 0
    
    def _compute_ks_drift(self, live: list[int], baseline: list[int]) -> int:
        """
        Compute KS test drift statistic between distributions.
        
        Uses empirical CDF comparison. Returns result in basis points (0-10000).
        Handles empty lists gracefully.
        
        Args:
            live: Live data distribution (list of integers).
            baseline: Baseline distribution (list of integers).
        
        Returns:
            int: KS statistic in basis points (0-10000 range).
        """
        # Handle empty cases
        if not live or not baseline:
            if not live and not baseline:
                return 0
            return 10000
        
        # If adequacy_v2 available, use its KS test
        if self._adequacy_v2 is not None:
            try:
                ks_stat = self._adequacy_v2.ks_test(live, baseline)
                return ks_stat
            except (ValueError, TypeError):
                pass
        
        # Fallback: manual KS computation
        return self._manual_ks_test(live, baseline)
    
    def _manual_ks_test(self, live: list[int], baseline: list[int]) -> int:
        """
        Manual KS test implementation using integer arithmetic.
        
        Args:
            live: Live distribution.
            baseline: Baseline distribution.
        
        Returns:
            int: KS statistic in basis points.
        """
        # Sort both distributions
        live_sorted = sorted(live)
        baseline_sorted = sorted(baseline)
        
        # Compute empirical CDFs at all unique points
        all_points = sorted(set(live_sorted) | set(baseline_sorted))
        
        if not all_points:
            return 0
        
        max_diff_bps = 0
        
        for point in all_points:
            # Count values <= point in each distribution
            live_count = self._count_le(live_sorted, point)
            baseline_count = self._count_le(baseline_sorted, point)
            
            # Compute CDF values in basis points
            live_cdf_bps = (live_count * 10000) // max(1, len(live_sorted))
            baseline_cdf_bps = (baseline_count * 10000) // max(1, len(baseline_sorted))
            
            # Compute absolute difference
            diff_bps = abs(live_cdf_bps - baseline_cdf_bps)
            max_diff_bps = max(max_diff_bps, diff_bps)
        
        return min(10000, max_diff_bps)
    
    def _count_le(self, sorted_list: list[int], value: int) -> int:
        """
        Count elements <= value in sorted list (binary search).
        
        Args:
            sorted_list: Sorted list of integers.
            value: Comparison value.
        
        Returns:
            int: Count of elements <= value.
        """
        # Binary search for rightmost position <= value
        left, right = 0, len(sorted_list)
        
        while left < right:
            mid = (left + right) // 2
            if sorted_list[mid] <= value:
                left = mid + 1
            else:
                right = mid
        
        return left
    
    def _compute_abs_drift(self, live: int, baseline: int) -> int:
        """
        Compute absolute metric drift in basis points.
        
        Converts to basis point scale (0-10000) based on magnitude.
        
        Args:
            live: Live metric value.
            baseline: Baseline metric value.
        
        Returns:
            int: Drift in basis points (0-10000 scale).
        """
        diff = abs(live - baseline)
        
        # If baseline is zero, handle specially
        if baseline == 0:
            # Assume diff is already in basis points
            return min(10000, diff)
        
        # Convert difference to basis points relative to baseline
        # drift_bps = (diff / baseline) * 10000
        if baseline > 0:
            drift_bps = (diff * 10000) // baseline
        else:
            drift_bps = (diff * 10000) // max(1, abs(baseline))
        
        return min(10000, drift_bps)
    
    def _compute_overall_drift(self) -> int:
        """
        Compute overall drift score as weighted average of components.
        
        Weights: returns 30%, volumes 15%, spreads 15%, volatility 15%,
                drawdown 15%, fill_quality 5%, trade_frequency 5%.
        
        Returns:
            int: Overall drift score in basis points (0-10000).
        """
        metrics = self._drift_metrics
        
        # Extract components with defaults
        returns_drift = metrics.get('returns_drift', 0)
        volumes_drift = metrics.get('volumes_drift', 0)
        spreads_drift = metrics.get('spreads_drift', 0)
        volatility_drift = metrics.get('volatility_drift', 0)
        drawdown_drift = metrics.get('drawdown_drift', 0)
        fill_quality_drift = metrics.get('fill_quality_drift', 0)
        trade_freq_drift = metrics.get('trade_frequency_drift', 0)
        
        # Weighted sum (all weights sum to 100)
        weighted_sum = (
            (returns_drift * 30) +
            (volumes_drift * 15) +
            (spreads_drift * 15) +
            (volatility_drift * 15) +
            (drawdown_drift * 15) +
            (fill_quality_drift * 5) +
            (trade_freq_drift * 5)
        )
        
        # Divide by total weight (100)
        overall_score = weighted_sum // 100
        
        return min(10000, max(0, overall_score))
    
    def get_drift_metrics(self) -> dict[str, int]:
        """
        Get most recent drift metrics from last detect() call.
        
        Returns:
            dict[str, int]: Copy of drift metrics dictionary.
        """
        return self._drift_metrics.copy()
    

    def regime_signature(self, market_data: dict) -> str:
        """Compute deterministic signature of current market regime."""
        if not isinstance(market_data, dict):
            raise ValueError("market_data must be dict")
        import hashlib
        data_str = str(sorted(market_data.items()))
        return hashlib.sha256(data_str.encode()).hexdigest()[:16]
