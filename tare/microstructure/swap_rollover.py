"""
swap_rollover.py — SwapRollover module for TARE.
Models FX swap rollover cost for overnight position carry.
Phase: 2 (Microstructure)
"""
class SwapRollover:
    @staticmethod
    def calculate_swap(position_size: int, swap_rate_pips: int, days: int) -> int:
        if not isinstance(position_size, int) or not isinstance(swap_rate_pips, int) or not isinstance(days, int):
            raise ValueError("position_size, swap_rate_pips, days must be integers")
        if days <= 0:
            raise ValueError("days must be positive")
        if position_size == 0:
            return 0
        return position_size * swap_rate_pips * days

    @staticmethod
    def is_rollover_day(day_of_week: int) -> bool:
        if not isinstance(day_of_week, int):
            raise ValueError("day_of_week must be integer")
        if day_of_week < 0 or day_of_week > 6:
            raise ValueError("day_of_week must be in range 0-6")
        return day_of_week == 2
