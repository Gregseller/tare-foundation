"""
execution_engine.py — Execution engine for TARE.
Phase: 3
"""
from tare.microstructure.latency_model import LatencyModel
from tare.time_engine.time_engine import TimeEngine

class ExecutionEngine:
    """Simulate order execution with realistic microstructure."""

    def __init__(self, base_latency_ns: int = 1000, slippage_factor: int = 1):
        if not isinstance(base_latency_ns, int) or base_latency_ns < 0:
            raise ValueError("base_latency_ns must be non-negative int")
        if not isinstance(slippage_factor, int) or slippage_factor < 0:
            raise ValueError("slippage_factor must be non-negative int")
        self._base_latency_ns = base_latency_ns
        self._slippage_factor = slippage_factor
        self._time_engine_inst = TimeEngine(base_latency_ns=base_latency_ns)
        self._latency_model = LatencyModel(profile={
            'base_latency_us': base_latency_ns // 1000,
            'time_engine': self._time_engine_inst
        })
        self._order_counter = 0
        self._orders = {}

    def submit_order(self, symbol: str, side: str, size: int, price: int) -> int:
        if not isinstance(symbol, str):
            raise ValueError("symbol must be str")
        if side not in ("buy", "sell"):
            raise ValueError("side must be \'buy\' or \'sell\'")
        if not isinstance(size, int) or size <= 0:
            raise ValueError("size must be positive int")
        if not isinstance(price, int) or price <= 0:
            raise ValueError("price must be positive int")

        self._order_counter += 1
        order_id = self._order_counter

        latency_ns = self._latency_model.simulate_latency(0)
        slippage = self._slippage_factor * size

        if side == "buy":
            executed_price = price + slippage
        else:
            executed_price = price - slippage
        if executed_price <= 0:
            executed_price = 1

        self._orders[order_id] = {
            "order_id":        order_id,
            "symbol":          symbol,
            "side":            side,
            "requested_size":  size,
            "requested_price": price,
            "executed_size":   size,
            "executed_price":  executed_price,
            "latency_ns":      latency_ns,
            "status":          "filled",
        }
        return order_id

    def get_execution_report(self, order_id: int) -> dict:
        if not isinstance(order_id, int):
            raise ValueError("order_id must be int")
        if order_id not in self._orders:
            raise ValueError("order_id not found")
        return self._orders[order_id].copy()

    def get_order_count(self) -> int:
        return self._order_counter
