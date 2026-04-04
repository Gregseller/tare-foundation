"""
adequacy_v1.py — Data adequacy validation for TARE.
Phase: 3+5
"""
class AdequacyV1:
    """Validate tick data sufficiency and quality for simulation."""

    def __init__(self, min_ticks: int = 1000, max_gap_ns: int = 3_600_000_000_000):
        if not isinstance(min_ticks, int) or min_ticks <= 0:
            raise ValueError("min_ticks must be positive int")
        if not isinstance(max_gap_ns, int) or max_gap_ns <= 0:
            raise ValueError("max_gap_ns must be positive int")
        self._min_ticks = min_ticks
        self._max_gap_ns = max_gap_ns

    def check_min_ticks(self, tick_count: int) -> bool:
        if not isinstance(tick_count, int):
            raise ValueError("tick_count must be int")
        return tick_count >= self._min_ticks

    def check_max_gap(self, timestamps: list[int]) -> bool:
        if not isinstance(timestamps, list):
            raise ValueError("timestamps must be list")
        if len(timestamps) <= 1:
            return True
        prev = timestamps[0]
        if not isinstance(prev, int):
            raise ValueError("timestamps must contain ints")
        for ts in timestamps[1:]:
            if not isinstance(ts, int):
                raise ValueError("timestamps must contain ints")
            if ts - prev > self._max_gap_ns:
                return False
            prev = ts
        return True

    def check_spread_sanity(self, spreads: list[int], max_spread: int) -> bool:
        if not isinstance(spreads, list):
            raise ValueError("spreads must be list")
        if not isinstance(max_spread, int) or max_spread <= 0:
            raise ValueError("max_spread must be positive int")
        for s in spreads:
            if not isinstance(s, int):
                raise ValueError("spreads must contain ints")
            if s < 0 or s > max_spread:
                return False
        return True

    def get_summary(self, tick_count: int, timestamps: list[int],
                    spreads: list[int], max_spread: int) -> dict:
        min_ok = self.check_min_ticks(tick_count)
        gap_ok = self.check_max_gap(timestamps)
        spread_ok = self.check_spread_sanity(spreads, max_spread)
        return {
            "tick_count":    tick_count,
            "min_ticks_ok":  min_ok,
            "max_gap_ok":    gap_ok,
            "spread_ok":     spread_ok,
            "adequate":      min_ok and gap_ok and spread_ok,
        }
