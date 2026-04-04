"""
websocket_connector.py — WebSocketConnector v1
TARE (Tick-Level Algorithmic Research Environment)

WebSocket bridge for real-time data and order execution.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Детерминизм: одни входные данные → всегда одинаковый результат
  - Только стандартная библиотека Python
"""

from typing import Optional, Generator, Callable
import json
import hashlib
from collections import deque


class WebSocketConnector:
    """
    WebSocket bridge for real-time data and order execution.
    
    Maintains deterministic websocket connections to exchange for streaming
    market data and order submission. All operations are deterministic with
    integer-only arithmetic. No float values, randomness, or external state.
    """
    
    def __init__(self, exchange_config: dict) -> None:
        """
        Initialize WebSocketConnector with exchange configuration.
        
        Args:
            exchange_config: Configuration dict with keys:
                - 'host' (str): WebSocket server hostname
                - 'port' (int): WebSocket server port
                - 'api_key' (str): API authentication key
                - 'api_secret' (str): API authentication secret
                - 'max_buffer_size' (int): Max messages in buffer (default 10000)
        
        Raises:
            ValueError: If required config keys are missing or invalid.
            TypeError: If config values have wrong types.
        """
        if not isinstance(exchange_config, dict):
            raise TypeError("exchange_config must be a dictionary")
        
        required_keys = ['host', 'port', 'api_key', 'api_secret']
        for key in required_keys:
            if key not in exchange_config:
                raise ValueError(f"exchange_config missing required key: {key}")
        
        self._host = exchange_config['host']
        self._port = exchange_config['port']
        self._api_key = exchange_config['api_key']
        self._api_secret = exchange_config['api_secret']
        
        if not isinstance(self._host, str) or not self._host:
            raise ValueError("host must be non-empty string")
        
        if not isinstance(self._port, int) or self._port <= 0:
            raise ValueError("port must be positive integer")
        
        if not isinstance(self._api_key, str) or not self._api_key:
            raise ValueError("api_key must be non-empty string")
        
        if not isinstance(self._api_secret, str) or not self._api_secret:
            raise ValueError("api_secret must be non-empty string")
        
        self._max_buffer_size = exchange_config.get('max_buffer_size', 10000)
        if not isinstance(self._max_buffer_size, int) or self._max_buffer_size <= 0:
            raise ValueError("max_buffer_size must be positive integer")
        
        # Connection state
        self._is_connected = False
        self._connection_time_us = 0
        self._subscribed_symbols = set()
        
        # Message buffer (FIFO)
        self._message_buffer = deque(maxlen=self._max_buffer_size)
        
        # Order tracking
        self._order_counter = 0
        self._pending_orders = {}  # order_id -> order dict
        self._order_responses = {}  # order_id -> response dict
        
        # Market data cache
        self._tick_cache = {}  # symbol -> latest tick
        self._tick_history = {}  # symbol -> list of ticks
        
        # Message handlers
        self._handlers = {}
        self._execution_engine = None
        
        # Deterministic state for testing
        self._deterministic_seed = self._compute_config_hash()
    
    def _compute_config_hash(self) -> int:
        """
        Compute deterministic hash of configuration.
        
        Returns:
            int: Hash value derived from config (deterministic).
        """
        config_str = f"{self._host}:{self._port}:{self._api_key}"
        hash_obj = hashlib.sha256(config_str.encode())
        hash_bytes = hash_obj.digest()
        # Take first 8 bytes as deterministic seed
        return int.from_bytes(hash_bytes[:8], byteorder='big')
    
    def connect(self) -> None:
        """
        Establish WebSocket connection to exchange.
        
        Opens persistent connection for real-time data streaming and order
        submission. In production would establish actual websocket, here
        simulates deterministically.
        
        Raises:
            RuntimeError: If connection fails or already connected.
        """
        if self._is_connected:
            raise RuntimeError("Already connected")
        
        # Validate host/port
        if not self._host or self._port <= 0:
            raise RuntimeError(f"Invalid host/port: {self._host}:{self._port}")
        
        # Simulate connection establishment
        self._is_connected = True
        self._connection_time_us = 0
        self._subscribed_symbols = set()
        self._message_buffer.clear()
        self._pending_orders.clear()
        self._order_counter = 0
    
    def disconnect(self) -> None:
        """
        Close WebSocket connection.
        
        Gracefully closes connection and cleans up resources.
        """
        self._is_connected = False
        self._subscribed_symbols.clear()
        self._message_buffer.clear()
    
    def is_connected(self) -> bool:
        """
        Check if WebSocket is currently connected.
        
        Returns:
            bool: True if connected, False otherwise.
        """
        return self._is_connected
    
    def subscribe(self, symbols: list[str]) -> None:
        """
        Subscribe to real-time market data for symbols.
        
        Initiates streaming of tick data for specified symbols. Subscription
        is deterministic and repeatable.
        
        Args:
            symbols: List of symbol strings (uppercase, e.g. ['EURUSD', 'GBPUSD']).
        
        Raises:
            RuntimeError: If not connected.
            ValueError: If symbols list is invalid.
            TypeError: If symbols have wrong type.
        """
        if not self._is_connected:
            raise RuntimeError("Not connected")
        
        if not isinstance(symbols, list):
            raise TypeError("symbols must be a list")
        
        if not symbols:
            raise ValueError("symbols list cannot be empty")
        
        for sym in symbols:
            if not isinstance(sym, str) or not sym:
                raise ValueError("Each symbol must be non-empty string")
            
            sym_upper = sym.upper()
            self._subscribed_symbols.add(sym_upper)
            
            # Initialize tick cache for this symbol
            if sym_upper not in self._tick_cache:
                self._tick_cache[sym_upper] = {}
                self._tick_history[sym_upper] = []
    
    def unsubscribe(self, symbols: list[str]) -> None:
        """
        Unsubscribe from market data for symbols.
        
        Args:
            symbols: List of symbol strings to unsubscribe from.
        
        Raises:
            RuntimeError: If not connected.
        """
        if not self._is_connected:
            raise RuntimeError("Not connected")
        
        if not isinstance(symbols, list):
            raise TypeError("symbols must be a list")
        
        for sym in symbols:
            if isinstance(sym, str):
                self._subscribed_symbols.discard(sym.upper())
    
    def _generate_order_id(self) -> int:
        """
        Generate deterministic order ID.
        
        Returns sequential integer IDs based on counter.
        
        Returns:
            int: Next order ID.
        """
        self._order_counter += 1
        return self._order_counter
    
    def _create_order_signature(self, order: dict) -> str:
        """
        Create deterministic signature for order.
        
        Args:
            order: Order dictionary.
        
        Returns:
            str: Hex signature string.
        """
        order_str = json.dumps(order, sort_keys=True)
        sig_input = f"{order_str}:{self._api_secret}"
        sig_hash = hashlib.sha256(sig_input.encode())
        return sig_hash.hexdigest()
    
    def send_order(self, order: dict) -> str:
        """
        Submit order for execution.
        
        Sends order to exchange via WebSocket. Returns order ID for tracking.
        Order processing is deterministic based on order parameters and
        connection state.
        
        Args:
            order: Order dict with keys:
                - 'symbol' (str): Asset symbol
                - 'side' (str): 'BUY' or 'SELL'
                - 'size' (int): Order size in units
                - 'order_type' (str): 'MARKET' or 'LIMIT'
                - 'price' (int, optional): Price for LIMIT orders
        
        Returns:
            str: Order ID as string for order tracking.
        
        Raises:
            RuntimeError: If not connected.
            ValueError: If order is invalid.
            TypeError: If order dict is malformed.
        """
        if not self._is_connected:
            raise RuntimeError("Not connected")
        
        if not isinstance(order, dict):
            raise TypeError("order must be a dictionary")
        
        # Validate required fields
        required = ['symbol', 'side', 'size', 'order_type']
        for field in required:
            if field not in order:
                raise ValueError(f"order missing required field: {field}")
        
        # Validate field types and values
        symbol = order.get('symbol', '')
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol must be non-empty string")
        
        side = order.get('side', '')
        if not isinstance(side, str) or side not in ['BUY', 'SELL']:
            raise ValueError("side must be 'BUY' or 'SELL'")
        
        size = order.get('size', 0)
        if not isinstance(size, int) or size <= 0:
            raise ValueError("size must be positive integer")
        
        order_type = order.get('order_type', '')
        if not isinstance(order_type, str) or order_type not in ['MARKET', 'LIMIT']:
            raise ValueError("order_type must be 'MARKET' or 'LIMIT'")
        
        # Check price for LIMIT orders
        if order_type == 'LIMIT':
            if 'price' not in order:
                raise ValueError("LIMIT orders must include price")
            price = order['price']
            if not isinstance(price, int) or price <= 0:
                raise ValueError("price must be positive integer")
        
        # Generate order ID
        order_id = self._generate_order_id()
        order_id_str = str(order_id)
        
        # Create order record
        order_record = {
            'order_id': order_id,
            'symbol': symbol.upper(),
            'side': side,
            'size': size,
            'order_type': order_type,
            'price': order.get('price', 0),
            'status': 'PENDING',
            'submission_time_us': 0,
            'signature': self._create_order_signature(order)
        }
        
        self._pending_orders[order_id] = order_record
        
        # Generate acceptance response
        response = {
            'order_id': order_id_str,
            'status': 'ACCEPTED',
            'timestamp_us': 0,
            'message': f'Order {order_id_str} accepted'
        }
        
        self._order_responses[order_id] = response
        
        # Buffer the response
        self._message_buffer.append({
            'type': 'ORDER_RESPONSE',
            'data': response
        })
        
        return order_id_str
    
    def cancel_order(self, order_id: str) -> bool:
        """
        Cancel pending order.
        
        Args:
            order_id: Order ID as string returned from send_order().
        
        Returns:
            bool: True if cancellation successful, False if order not found.
        
        Raises:
            RuntimeError: If not connected.
            ValueError: If order_id is invalid format.
        """
        if not self._is_connected:
            raise RuntimeError("Not connected")
        
        if not isinstance(order_id, str) or not order_id:
            raise ValueError("order_id must be non-empty string")
        
        try:
            order_id_int = int(order_id)
        except ValueError:
            raise ValueError("order_id must be valid integer string")
        
        if order_id_int not in self._pending_orders:
            return False
        
        order = self._pending_orders[order_id_int]
        order['status'] = 'CANCELLED'
        
        # Buffer cancellation response
        self._message_buffer.append({
            'type': 'ORDER_CANCELLED',
            'data': {
                'order_id': order_id,
                'status': 'CANCELLED',
                'timestamp_us': 0
            }
        })
        
        return True
    
    def recv_message(self) -> Optional[dict]:
        """
        Receive next message from WebSocket buffer.
        
        Returns buffered messages (market data or order responses) in FIFO order.
        Simulates message reception without actual network I/O.
        
        Returns:
            dict: Message dict or None if buffer empty.
                  Message format depends on type:
                  - 'TICK': {'type': 'TICK', 'data': tick_dict}
                  - 'ORDER_RESPONSE': {'type': 'ORDER_RESPONSE', 'data': response_dict}
                  - 'ORDER_CANCELLED': {'type': 'ORDER_CANCELLED', 'data': response_dict}
        
        Raises:
            RuntimeError: If not connected.
        """
        if not self._is_connected:
            raise RuntimeError("Not connected")
        
        if self._message_buffer:
            return self._message_buffer.popleft()
        
        return None
    
    def inject_tick(self, tick: dict) -> None:
        """
        Inject market tick into receive buffer.
        
        Used for testing/simulation to feed market data into the connector.
        Tick is automatically buffered for recv_message().
        
        Args:
            tick: Tick dict with keys:
                - 'symbol' (str): Asset symbol
                - 'price' (int): Last price
                - 'bid' (int): Bid price
                - 'ask' (int): Ask price
                - 'volume' (int): Tick volume
        
        Raises:
            RuntimeError: If not connected.
            ValueError: If tick is invalid.
        """
        if not self._is_connected:
            raise RuntimeError("Not connected")
        
        if not isinstance(tick, dict):
            raise ValueError("tick must be a dictionary")
        
        required = ['symbol', 'price', 'bid', 'ask', 'volume']
        for field in required:
            if field not in tick:
                raise ValueError(f"tick missing field: {field}")
        
        symbol = tick.get('symbol', '')
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol must be non-empty string")
        
        symbol_upper = symbol.upper()
        
        # Cache the tick
        self._tick_cache[symbol_upper] = tick
        self._tick_history[symbol_upper].append(tick)
        
        # Keep only last 1000 ticks per symbol
        if len(self._tick_history[symbol_upper]) >
