"""
fix_connector.py - FIX Protocol connector for TARE.
Phase: 7
"""
import hashlib
from collections import deque


class FIXConnector:
    """Simulate FIX protocol connection to broker."""

    def __init__(self, broker_config: dict) -> None:
        if not isinstance(broker_config, dict):
            raise ValueError("broker_config must be dict")
        for key in ("sender_comp_id", "target_comp_id"):
            if key not in broker_config:
                raise ValueError(f"broker_config missing {key!r}")
        if not isinstance(broker_config["sender_comp_id"], str):
            raise ValueError("sender_comp_id must be str")
        if not isinstance(broker_config["target_comp_id"], str):
            raise ValueError("target_comp_id must be str")

        self._sender = broker_config["sender_comp_id"]
        self._target = broker_config["target_comp_id"]
        self._connected = False
        self._subscribed = set()
        self._order_counter = 0
        self._orders = {}
        self._message_queue = deque()

    def connect(self) -> None:
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def _require_connected(self):
        if not self._connected:
            raise RuntimeError("Not connected")

    def subscribe(self, symbols: list) -> None:
        self._require_connected()
        if not isinstance(symbols, list):
            raise ValueError("symbols must be list")
        for s in symbols:
            if not isinstance(s, str):
                raise ValueError("each symbol must be str")
        for s in symbols:
            self._subscribed.add(s)

    def unsubscribe(self, symbols: list) -> None:
        self._require_connected()
        if not isinstance(symbols, list):
            raise ValueError("symbols must be list")
        for s in symbols:
            self._subscribed.discard(s)

    def _generate_order_id(self, order: dict) -> str:
        data = f"{self._sender}{self._order_counter}{order}"
        return hashlib.sha256(data.encode()).hexdigest()[:12]

    def send_order(self, order: dict) -> str:
        self._require_connected()
        if not isinstance(order, dict):
            raise ValueError("order must be dict")
        for key in ("symbol", "side", "size", "price"):
            if key not in order:
                raise ValueError(f"order missing key {key!r}")
        if order["side"] not in ("buy", "sell"):
            raise ValueError("side must be buy or sell")
        if not isinstance(order["size"], int) or order["size"] <= 0:
            raise ValueError("size must be positive int")
        if not isinstance(order["price"], int) or order["price"] <= 0:
            raise ValueError("price must be positive int")

        self._order_counter += 1
        order_id = self._generate_order_id(order)
        self._orders[order_id] = order.copy()

        self._message_queue.append({
            "type": "execution",
            "order_id": order_id,
            "symbol": order["symbol"],
            "status": "filled",
        })
        return order_id

    def cancel_order(self, order_id: str) -> bool:
        self._require_connected()
        if not isinstance(order_id, str):
            raise ValueError("order_id must be str")
        if order_id not in self._orders:
            return False
        del self._orders[order_id]
        return True

    def inject_tick(self, tick: dict) -> None:
        if not isinstance(tick, dict):
            raise ValueError("tick must be dict")
        for key in ("symbol", "bid", "ask", "timestamp"):
            if key not in tick:
                raise ValueError(f"tick missing key {key!r}")
        self._message_queue.append({
            "type": "tick",
            "symbol": tick["symbol"],
            "bid": tick["bid"],
            "ask": tick["ask"],
            "timestamp": tick["timestamp"],
        })

    def recv_message(self):
        if not self._message_queue:
            return None
        return self._message_queue.popleft()
