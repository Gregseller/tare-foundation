"""
queue_position.py — QueuePosition module for TARE.
Estimates queue position and fill time based on market depth and tick rate.
Phase: 2 (Microstructure)
"""
class QueuePosition:
    """Deterministic queue position estimator for limit orders."""

    @staticmethod
    def estimate_queue_ahead(order_size: int, depth_levels: list[tuple[int, int]]) -> int:
        if not isinstance(order_size, int) or order_size <= 0:
            return 0
        if not isinstance(depth_levels, list):
            return 0
        ahead = 0
        for price, volume in depth_levels:
            if not isinstance(price, int) or not isinstance(volume, int):
                continue
            if volume <= 0:
                continue
            ahead += volume
        return min(ahead, 2**63 - 1)

    @staticmethod
    def estimate_fill_time(queue_ahead: int, tick_rate: int) -> int:
        if not isinstance(queue_ahead, int) or queue_ahead <= 0:
            return 0
        if not isinstance(tick_rate, int) or tick_rate <= 0:
            return 0
        return max(0, queue_ahead // tick_rate)
