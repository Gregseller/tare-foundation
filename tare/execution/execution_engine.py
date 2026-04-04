"""
execution_engine.py — ExecutionEngine v1
TARE (Tick-Level Algorithmic Research Environment)

Execute orders with realistic microstructure and account for market impact.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Детерминизм: одни входные данные → всегда одинаковый результат
  - Только стандартная библиотека Python
"""

from typing import Optional, Generator


class ExecutionEngine:
    """
    Execute orders with realistic microstructure and account for market impact.
    
    Deteministically simulates order submission, latency, slippage, and partial fills.
    Tracks order lifecycle from submission through execution with full accounting
    for market impact, latency delays, and realistic fill patterns.
    All operations use integer arithmetic only.
    """
    
    def __init__(
        self,
        tick_data_engine,
        latency_model,
        slippage_engine,
        partial_fills_simulator
    ):
        """
        Initialize ExecutionEngine with dependencies.
        
        Args:
            tick_data_engine: TickDataEngine instance for market data access.
            latency_model: LatencyModel instance for latency simulation.
            slippage_engine: SlippageEngine instance for slippage calculation.
            partial_fills_simulator: PartialFillSimulator for fill simulation.
            
        Raises:
            ValueError: If any dependency is None or invalid.
        """
        if tick_data_engine is None:
            raise ValueError("tick_data_engine cannot be None")
        if latency_model is None:
            raise ValueError("latency_model cannot be None")
        if slippage_engine is None:
            raise ValueError("slippage_engine cannot be None")
        if partial_fills_simulator is None:
            raise ValueError("partial_fills_simulator cannot be None")
        
        self._tick_data_engine = tick_data_engine
        self._latency_model = latency_model
        self._slippage_engine = slippage_engine
        self._partial_fills = partial_fills_simulator
        
        self._orders = {}  # order_id -> order_state dict
        self._order_counter = 0
        self._execution_reports = {}  # order_id -> execution report dict
        self._market_state = {}  # symbol -> current market state
    
    def _generate_order_id(self) -> int:
        """
        Generate deterministic order ID.
        
        Returns sequential integer IDs starting from 1. Deterministic
        across calls due to simple counter increment.
        
        Returns:
            int: Next order ID in sequence.
        """
        self._order_counter += 1
        return self._order_counter
    
    def _validate_order_params(
        self,
        symbol: str,
        side: str,
        size: int,
        order_type: str
    ) -> None:
        """
        Validate order submission parameters.
        
        Args:
            symbol: Asset symbol (string).
            side: Order side 'BUY' or 'SELL'.
            size: Order size (positive integer).
            order_type: Order type 'MARKET' or 'LIMIT'.
            
        Raises:
            ValueError: If any parameter is invalid.
            TypeError: If parameters have wrong types.
        """
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol must be non-empty string")
        
        if not isinstance(side, str) or side not in ['BUY', 'SELL']:
            raise ValueError("side must be 'BUY' or 'SELL'")
        
        if not isinstance(size, int) or size <= 0:
            raise ValueError("size must be positive integer")
        
        if not isinstance(order_type, str) or order_type not in ['MARKET', 'LIMIT']:
            raise ValueError("order_type must be 'MARKET' or 'LIMIT'")
    
    def _update_market_state(self, symbol: str, tick_data: dict) -> None:
        """
        Update internal market state from tick data.
        
        Args:
            symbol: Asset symbol.
            tick_data: Tick dictionary with price and volume data.
        """
        if symbol not in self._market_state:
            self._market_state[symbol] = {
                'last_price': 0,
                'bid': 0,
                'ask': 0,
                'spread_history': [],
                'volume_profile': {}
            }
        
        state = self._market_state[symbol]
        
        # Update last price
        if 'price' in tick_data:
            state['last_price'] = tick_data['price']
        
        # Estimate bid/ask from price and volume patterns
        if 'volume' in tick_data:
            volume = tick_data['volume']
            price = tick_data.get('price', state['last_price'])
            
            # Simplistic: bid is price - 50bps, ask is price + 50bps
            # In reality would come from order book snapshot
            state['bid'] = price - 50 if price > 50 else price // 2
            state['ask'] = price + 50
            
            spread = state['ask'] - state['bid']
            state['spread_history'].append(spread)
            
            # Keep only last 100 spreads
            if len(state['spread_history']) > 100:
                state['spread_history'] = state['spread_history'][-100:]
    
    def _get_market_depth(self, symbol: str, size: int) -> dict:
        """
        Construct market depth for order matching.
        
        Returns deterministic market depth based on symbol's market state.
        
        Args:
            symbol: Asset symbol.
            size: Order size for depth estimation.
            
        Returns:
            dict: Market depth with 'bids' and 'asks' lists of (price, volume) tuples.
        """
        if symbol not in self._market_state:
            # Default depth if no market state
            return {
                'bids': [(10000, size)],
                'asks': [(10001, size)]
            }
        
        state = self._market_state[symbol]
        bid = state.get('bid', 10000)
        ask = state.get('ask', 10001)
        
        # Build depth with 5 levels on each side
        bids = []
        asks = []
        
        bid_price = bid
        for i in range(5):
            level_volume = size // (i + 2)  # Decreasing volume at worse prices
            if level_volume > 0:
                bids.append((bid_price, level_volume))
            bid_price -= 10
        
        ask_price = ask
        for i in range(5):
            level_volume = size // (i + 2)
            if level_volume > 0:
                asks.append((ask_price, level_volume))
            ask_price += 10
        
        return {'bids': bids, 'asks': asks}
    
    def submit_order(
        self,
        symbol: str,
        side: str,
        size: int,
        order_type: str
    ) -> int:
        """
        Submit an order for execution.
        
        Validates order parameters and initiates execution workflow.
        Returns order ID for tracking. All execution happens deterministically
        based on input parameters and current market state.
        
        Args:
            symbol: Asset symbol (uppercase string, e.g. 'EURUSD').
            side: Order side - 'BUY' or 'SELL'.
            size: Order size in units (positive integer).
            order_type: Order type - 'MARKET' or 'LIMIT'.
            
        Returns:
            int: Order ID for tracking and report retrieval.
            
        Raises:
            ValueError: If order parameters are invalid.
            TypeError: If parameters have wrong types.
        """
        self._validate_order_params(symbol, side, size, order_type)
        
        order_id = self._generate_order_id()
        
        # Create order state
        order_state = {
            'order_id': order_id,
            'symbol': symbol.upper(),
            'side': side,
            'size': size,
            'order_type': order_type,
            'submission_time_us': 0,  # Deterministic: always 0 relative start
            'status': 'SUBMITTED',
            'filled_volume': 0,
            'fills': [],
            'market_depth': self._get_market_depth(symbol.upper(), size)
        }
        
        # Simulate latency
        submission_time_us = 0
        latency_us = self._latency_model.simulate_latency(submission_time_us)
        execution_time_us = submission_time_us + latency_us
        
        order_state['latency_us'] = latency_us
        order_state['execution_time_us'] = execution_time_us
        
        # Process fills
        self._process_order_fills(order_state)
        
        # Calculate slippage
        order_state['slippage_bps'] = self._calculate_order_slippage(order_state)
        
        # Store order and report
        self._orders[order_id] = order_state
        self._generate_execution_report(order_id, order_state)
        
        return order_id
    
    def _process_order_fills(self, order_state: dict) -> None:
        """
        Process partial fills for an order.
        
        Uses PartialFillSimulator to match order against market depth,
        generating list of (price, volume) fills.
        
        Args:
            order_state: Order state dictionary to update with fills.
        """
        size = order_state['size']
        depth = order_state['market_depth']
        
        fills = self._partial_fills.fill_order(size, depth)
        
        order_state['fills'] = fills
        
        # Sum filled volume
        filled_volume = 0
        for price, volume in fills:
            filled_volume += volume
        
        order_state['filled_volume'] = filled_volume
        
        # Set status
        if filled_volume >= size:
            order_state['status'] = 'FILLED'
        elif filled_volume > 0:
            order_state['status'] = 'PARTIALLY_FILLED'
        else:
            order_state['status'] = 'REJECTED'
    
    def _calculate_order_slippage(self, order_state: dict) -> int:
        """
        Calculate slippage for an order.
        
        Uses SlippageEngine to compute realistic slippage based on order size
        and market conditions.
        
        Args:
            order_state: Order state dict with fills and market data.
            
        Returns:
            int: Total slippage in basis points.
        """
        symbol = order_state['symbol']
        
        if symbol not in self._market_state:
            return 0
        
        state = self._market_state[symbol]
        bid = state.get('bid', 10000)
        ask = state.get('ask', 10001)
        spread_hist = state.get('spread_history', [])
        
        size = order_state['size']
        
        slippage = self._slippage_engine.compute_slippage(
            order_size=size,
            bid=bid,
            ask=ask,
            spread_history=spread_hist
        )
        
        return slippage
    
    def _generate_execution_report(self, order_id: int, order_state: dict) -> None:
        """
        Generate execution report for an order.
        
        Args:
            order_id: Order ID.
            order_state: Order state dictionary.
        """
        fills = order_state.get('fills', [])
        
        # Calculate VWAP
        vwap = 0
        if fills:
            stats = self._partial_fills.calculate_fill_statistics(fills)
            vwap = stats.get('vwap', 0)
        
        report = {
            'order_id': order_id,
            'symbol': order_state['symbol'],
            'side': order_state['side'],
            'order_size': order_state['size'],
            'filled_volume': order_state['filled_volume'],
            'status': order_state['status'],
            'latency_us': order_state['latency_us'],
            'execution_time_us': order_state['execution_time_us'],
            'slippage_bps': order_state['slippage_bps'],
            'vwap': vwap,
            'fills': fills,
            'num_fill_levels': len(fills)
        }
        
        self._execution_reports[order_id] = report
    
    def get_execution_report(self, order_id: int) -> dict:
        """
        Retrieve execution report for an order.
        
        Returns comprehensive execution details including fills, slippage,
        latency, and final status.
        
        Args:
            order_id: Order ID returned from submit_order().
            
        Returns:
            dict: Execution report with keys:
                - order_id (int)
                - symbol (str)
                - side (str)
                - order_size (int)
                - filled_volume (int)
                - status (str): 'FILLED', 'PARTIALLY_FILLED', 'REJECTED'
                - latency_us (int): Order submission latency in microseconds
                - execution_time_us (int): Absolute execution time
                - slippage_bps (int): Realized slippage in basis points
                - vwap (int): Volume-weighted average price
                - fills (list): List of (price, volume) tuples
                - num_fill_levels (int): Number of price levels executed at
                
            Empty dict if order_id not found.
            
        Raises:
            ValueError: If order_id is not an integer.
        """
        if not isinstance(order_id, int):
            raise ValueError("order_id must be an integer")
        
        if order_id not in self._execution_reports:
            return {}
        
        return self._execution_reports[order_id].copy()
    
    def get_order_status(self, order_id: int) -> str:
        """
        Get current status of an order.
        
        Args:
            order_id: Order ID.
            
        Returns:
            str: Order status ('SUBMITTED', 'FILLED', 'PARTIALLY_FILLED', 'REJECTED')
                 or empty string if order not found.
        """
        if not isinstance(order_id, int):
            raise ValueError("order_id must be an integer")
        
        if order_id not in self._orders:
            return ""
        
        return self._orders[order_id].get('status', '')
    
    def update_market_snapshot(self, ticks: list) -> None:
        """
        Update market state from tick data snapshot.
        
        Processes list of ticks to maintain current market view for realistic
        execution simulation. Call before submitting orders for best accuracy.
        
        Args:
            ticks: List of tick dictionaries with symbol, price, volume fields.
        """
        if not isinstance(ticks, list):
            raise ValueError("ticks must be a list")
        
        for tick in ticks:
            if not isinstance(tick, dict):
                continue
            
            symbol = tick.get('symbol', '')
            if symbol:
                self._update_market_state(symbol, tick)
    
    def stream_order_fills(self, order_id: int) -> Generator[tuple[int, int], None, None]:
        """
        Stream fills for an order one at a time.
        
        Lazily yields (price, volume) tuples without building entire list.
        Useful for processing large orders.
        
        Args:
            order_id:
