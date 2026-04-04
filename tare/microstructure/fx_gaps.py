"""
fx_gaps.py — FXGaps module for TARE.
Detects and models price gaps in FX market.
Phase: 2 (Microstructure)
"""
class FXGaps:
    """Detects and models price gaps in FX market."""
    @staticmethod
    def detect_gap(prev_close: int, current_open: int) -> int:
        if not isinstance(prev_close, int) or not isinstance(current_open, int):
            raise ValueError("prev_close and current_open must be integers")
        return current_open - prev_close

    @staticmethod
    def is_significant(gap_size: int, threshold: int) -> bool:
        if not isinstance(gap_size, int) or not isinstance(threshold, int):
            raise ValueError("gap_size and threshold must be integers")
        if threshold <= 0:
            raise ValueError("threshold must be positive")
        return abs(gap_size) >= threshold

    @staticmethod
    def adjust_for_gap(price: int, gap_size: int) -> int:
        if not isinstance(price, int) or not isinstance(gap_size, int):
            raise ValueError("price and gap_size must be integers")
        return price + gap_size
