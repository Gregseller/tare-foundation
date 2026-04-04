"""
slippage_engine.py — SlippageEngine v1
TARE (Tick-Level Algorithmic Research Environment)

Calculate realistic slippage based on order size and market state.
Deterministic, integer-only, no randomness.
"""

from typing import Optional


class SlippageEngine:
    """
    Calculates realistic slippage based on order size and market state.

    Rules:
      - Only int, no float
      - No randomness
      - Determinism: same inputs → same result
      - Uses spread history to model market impact
    """

    def __init__(self, tick_data_engine, latency_model):
        """
        Initialize SlippageEngine.

        Args:
            tick_data_engine: Instance of TickDataEngine for market data.
            latency_model: Instance of LatencyModel for latency simulation.

        Raises:
            ValueError: If dependencies are invalid.
        """
        # We don't need to import the actual classes, just check they exist
        if tick_data_engine is None:
            raise ValueError("tick_data_engine cannot be None")
        if latency_model is None:
            raise ValueError("latency_model cannot be None")

        self._tick_data_engine = tick_data_engine
        self._latency_model = latency_model

    def compute_slippage(self, order_size: int, bid: int, ask: int,
                         spread_history: list[int]) -> int:
        """
        Calculate slippage in price units for a given order.

        Algorithm:
          1. Calculate current spread = ask - bid
          2. Calculate average spread from history (if available)
          3. Determine market impact factor based on order size relative to spread
          4. Apply latency-based adjustment
          5. Return slippage as integer price units

        Args:
            order_size: Order size in units (positive int).
            bid: Current best bid price (int).
            ask: Current best ask price (int).
            spread_history: List of recent spreads (list of ints).

        Returns:
            Slippage in price units (int). Positive means worse price.

        Raises:
            ValueError: If inputs are invalid.
        """
        # Input validation
        if not isinstance(order_size, int) or order_size <= 0:
            raise ValueError("order_size must be a positive int")
        if not isinstance(bid, int) or bid <= 0:
            raise ValueError("bid must be a positive int")
        if not isinstance(ask, int) or ask <= 0:
            raise ValueError("ask must be a positive int")
        if ask <= bid:
            raise ValueError("ask must be greater than bid")
        if not isinstance(spread_history, list):
            raise ValueError("spread_history must be a list")
        if not all(isinstance(s, int) and s >= 0 for s in spread_history):
            raise ValueError("All spread_history values must be non-negative ints")

        # 1. Current spread
        current_spread = ask - bid

        # 2. Average spread from history (if available)
        avg_spread = self._calculate_average_spread(spread_history, current_spread)

        # 3. Market impact factor
        # Base impact: order_size relative to spread
        # Larger orders relative to spread cause more slippage
        if avg_spread > 0:
            size_ratio = (order_size * 100) // avg_spread  # Scale up to avoid small numbers
        else:
            size_ratio = order_size * 100  # If spread is 0, use order_size directly

        # Impact factor: sqrt-like function using integer math
        # We approximate sqrt(size_ratio) * spread / 100
        impact = self._integer_sqrt(size_ratio) * avg_spread // 100

        # 4. Latency adjustment
        # Simulate latency to get execution time
        # Use current time from tick data engine if available, else use 0
        try:
            stats = self._tick_data_engine.get_stats()
            current_time_us = stats.get('timestamp_range', (0, 0))[1] // 1000  # ns to us
        except (ValueError, AttributeError):
            current_time_us = 0

        execution_time_us = self._latency_model.simulate_latency(current_time_us)

        # Latency penalty: more latency → more slippage
        # Base: 1 unit per 1000us of latency
        latency_penalty = execution_time_us // 1000

        # 5. Total slippage
        total_slippage = impact + latency_penalty

        # Ensure slippage is non-negative and reasonable (not more than 10x spread)
        max_slippage = current_spread * 10
        if total_slippage > max_slippage:
            total_slippage = max_slippage

        return total_slippage

    def _calculate_average_spread(self, spread_history: list[int],
                                  current_spread: int) -> int:
        """
        Calculate average spread from history.

        Args:
            spread_history: List of recent spreads.
            current_spread: Current spread.

        Returns:
            Average spread as integer.
        """
        if not spread_history:
            return current_spread

        # Use weighted average: recent spreads matter more
        total = 0
        weight_sum = 0

        for i, spread in enumerate(spread_history[-10:]):  # Last 10 at most
            weight = 10 - i  # More weight to recent
            if weight <= 0:
                weight = 1
            total += spread * weight
            weight_sum += weight

        # Include current spread with highest weight
        total += current_spread * 15
        weight_sum += 15

        return total // weight_sum

    def _integer_sqrt(self, n: int) -> int:
        """
        Calculate integer square root using Babylonian method.

        Args:
            n: Non-negative integer.

        Returns:
            Integer square root.
        """
        if n <= 1:
            return n

        x = n
        y = (x + 1) // 2

        while y < x:
            x = y
            y = (x + n // x) // 2

        return x

    # ------------------------------------------------------------------
    # Properties (read-only)
    # ------------------------------------------------------------------

    @property
    def tick_data_engine(self):
        """Get the tick data engine instance."""
        return self._tick_data_engine

    @property
    def latency_model(self):
        """Get the latency model instance."""
        return self._latency_model


# ------------------------------------------------------------------
# Self-test
# ------------------------------------------------------------------

if __name__ == "__main__":
    print("=== SlippageEngine v1 — self-test ===\n")

    # Mock dependencies
    class MockTickDataEngine:
        def get_stats(self):
            return {'timestamp_range': (0, 1000000000)}  # 1 second in ns

    class MockLatencyModel:
        def simulate_latency(self, order_time_us):
            return order_time_us + 50  # 50us latency

    # Create engine
    tick_engine = MockTickDataEngine()
    latency_model = MockLatencyModel()
    engine = SlippageEngine(tick_engine, latency_model)

    # Test 1: Basic calculation
    order_size = 100
    bid = 10000
    ask = 10010  # spread = 10
    spread_history = [8, 9, 10, 11, 12]

    slippage = engine.compute_slippage(order_size, bid, ask, spread_history)
    print(f"  ✓ Basic calculation: slippage = {slippage}")
    assert isinstance(slippage, int), "Slippage must be int"
    assert slippage >= 0, "Slippage must be non-negative"

    # Test 2: Determinism
    slippage2 = engine.compute_slippage(order_size, bid, ask, spread_history)
    assert slippage == slippage2, "Determinism violated"
    print("  ✓ Determinism confirmed")

    # Test 3: Larger order size
    large_slippage = engine.compute_slippage(1000, bid, ask, spread_history)
    assert large_slippage >= slippage, "Larger order should have equal or more slippage"
    print(f"  ✓ Larger order size: {large_slippage} >= {slippage}")

    # Test 4: Zero spread history
    slippage3 = engine.compute_slippage(order_size, bid, ask, [])
    assert isinstance(slippage3, int), "Must handle empty history"
    print(f"  ✓ Empty spread history: {slippage3}")

    # Test 5: Invalid inputs
    try:
        engine.compute_slippage(-100, bid, ask, spread_history)
        assert False, "Should reject negative order size"
    except ValueError:
        print("  ✓ Rejects negative order size")

    try:
        engine.compute_slippage(order_size, ask, bid, spread_history)  # ask <= bid
        assert False, "Should reject ask <= bid"
    except ValueError:
        print("  ✓ Rejects ask <= bid")

    print("\n=== PASSED ===")
