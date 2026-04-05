"""
websocket_connector.py - WebSocket connector for TARE.
Phase: 7
"""
import hashlib
from collections import deque


class WebSocketConnector:
    """Simulate WebSocket connection to exchange."""

    def __init__(self, exchange_config: dict) -> None:
        if not isinstance(exchange_config, dict):
            raise ValueError("exchange_config must be dict")
        for key in ("host", "port", "api_key", "api_secret"):
            if key not in exchange_config:
                raise ValueError(f"exchange_config missing key {key!r}")
        if not isinstance(exchange_config["host"], str) or not exchange_config["host"]:
            raise ValueError("host must be non-empty str")
        if not isinstance(exchange_config["port"], int) or exchange_config["port"] <= 0:
            raise ValueError("port must be positive int")
        if not isinstance(exchange_config["api_key"], str) or not exchange_config["api_key"]:
            raise ValueError("api_key must be non-empty str")
        if not isinstance(exchange_config["api_secret"], str) or not exchange_config["api_secret"]:
            raise ValueError("api_secret must be non-empty str")
        max_buf = exchange_config.get("max_buffer_size", 10000)
        if not isinstance(max_buf, int) or max_buf <= 0:
            raise ValueError("max_buffer_size must be positive int")

        self._host = exchange_config["host"]
        self._port = exchange_config["port"]
        self._api_key = exchange_config["api_key"]
        self._connected = False
        self._subscribed = set()
        self._order_counter = 0
        self._orders = {}
        self._message_queue = deque(maxlen=max_buf)

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
        data = f"{self._host}{self._port}{self._api_key}{self._order_counter}{order}"
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
