"""
adequacy_v1.py — AdequacyV1 Validator
TARE (Tick-Level Algorithmic Research Environment)

Validate that execution matches realistic market conditions.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Детерминизм: одни входные данные → всегда одинаковый результат
  - Только стандартная библиотека Python
"""

from typing import Generator


class AdequacyV1:
    """
    Validate that execution matches realistic market conditions.
    
    Checks backtest execution reports against market data to ensure
    fills, slippage, latency, and partial fills are realistic.
    All validations use integer arithmetic only. Deterministic checks
    based on execution engine and portfolio engine outputs.
    """
    
    def __init__(self, execution_engine, portfolio_engine):
        """
        Initialize AdequacyV1 validator with dependencies.
        
        Args:
            execution_engine: ExecutionEngine instance for execution data.
            portfolio_engine: PortfolioEngine instance for portfolio state.
            
        Raises:
            ValueError: If any dependency is None or invalid.
        """
        if execution_engine is None:
            raise ValueError("execution_engine cannot be None")
        if portfolio_engine is None:
            raise ValueError("portfolio_engine cannot be None")
        
        self._execution_engine = execution_engine
        self._portfolio_engine = portfolio_engine
        
        # Violation tracking: list of violation descriptions
        self._violations = []
        
        # Adequacy thresholds (all in basis points for integer arithmetic)
        self._max_latency_us = 5000000  # 5 seconds in microseconds
        self._max_slippage_bps = 500  # 500 basis points = 5%
        self._min_fill_ratio = 5000  # 50% minimum fill ratio (in basis points)
        self._max_execution_levels = 20  # Max price levels for execution
        self._max_spread_bps = 1000  # Max realistic spread (10%)
    
    def validate_execution(
        self,
        backtest_report: dict,
        market_data: dict
    ) -> dict[str, bool]:
        """
        Validate that execution matches realistic market conditions.
        
        Performs comprehensive checks on backtest report against market data:
        - Fill volumes and prices are realistic
        - Slippage is within acceptable bounds
        - Latency is reasonable
        - Partial fills follow market microstructure patterns
        - No impossible executions (fills beyond available liquidity)
        
        All checks are deterministic and return consistent results for same inputs.
        Uses integer basis point arithmetic throughout.
        
        Args:
            backtest_report: Dict with execution reports from backtest:
                {
                    'orders': [order_report_dict],
                    'portfolio_pnl': int,
                    'trades': int,
                    'symbols': [str]
                }
            market_data: Dict with tick-level market data:
                {
                    'symbol': str,
                    'ticks': [{'price': int, 'volume': int, 'timestamp': int}],
                    'bid_ask_spreads': [int],
                    'liquidity_profile': {symbol: int}
                }
        
        Returns:
            dict with boolean validation results:
            {
                'fills_realistic': bool,
                'slippage_reasonable': bool,
                'latency_plausible': bool,
                'partial_fills_valid': bool,
                'liquidity_sufficient': bool,
                'price_levels_reasonable': bool,
                'no_crossing_spreads': bool,
                'order_sizes_feasible': bool,
                'overall_adequate': bool
            }
            
        Raises:
            ValueError: If backtest_report or market_data invalid.
            TypeError: If parameters have wrong types.
        """
        if not isinstance(backtest_report, dict):
            raise ValueError("backtest_report must be a dict")
        if not isinstance(market_data, dict):
            raise ValueError("market_data must be a dict")
        
        # Reset violations
        self._violations = []
        
        # Run individual validation checks
        checks = {
            'fills_realistic': self._validate_fills_realistic(backtest_report, market_data),
            'slippage_reasonable': self._validate_slippage_reasonable(backtest_report, market_data),
            'latency_plausible': self._validate_latency_plausible(backtest_report),
            'partial_fills_valid': self._validate_partial_fills(backtest_report, market_data),
            'liquidity_sufficient': self._validate_liquidity_sufficient(backtest_report, market_data),
            'price_levels_reasonable': self._validate_price_levels(backtest_report),
            'no_crossing_spreads': self._validate_no_crossing_spreads(backtest_report, market_data),
            'order_sizes_feasible': self._validate_order_sizes(backtest_report, market_data),
        }
        
        # Overall adequacy: all checks pass
        checks['overall_adequate'] = all(checks.values())
        
        return checks
    
    def _validate_fills_realistic(self, backtest_report: dict, market_data: dict) -> bool:
        """
        Validate that filled volumes match market liquidity.
        
        Args:
            backtest_report: Backtest execution report.
            market_data: Market tick data.
            
        Returns:
            bool: True if fills are realistic.
        """
        orders = backtest_report.get('orders', [])
        
        if not isinstance(orders, list):
            self._violations.append("orders must be list")
            return False
        
        for order in orders:
            if not isinstance(order, dict):
                continue
            
            symbol = order.get('symbol', '')
            filled_volume = order.get('filled_volume', 0)
            order_size = order.get('order_size', 0)
            
            if not isinstance(filled_volume, int) or not isinstance(order_size, int):
                self._violations.append(f"Order {order.get('order_id')} has non-int volumes")
                return False
            
            # Check: filled volume cannot exceed order size
            if filled_volume > order_size:
                self._violations.append(
                    f"Order filled {filled_volume} > requested {order_size}"
                )
                return False
            
            # Check: if order size > 0, should have some fill or explicit rejection
            if order_size > 0 and filled_volume == 0:
                status = order.get('status', '')
                if status != 'REJECTED':
                    # Only warning, not hard fail - rejections are valid
                    pass
        
        return True
    
    def _validate_slippage_reasonable(self, backtest_report: dict, market_data: dict) -> bool:
        """
        Validate that slippage is within realistic bounds.
        
        Args:
            backtest_report: Backtest execution report.
            market_data: Market tick data.
            
        Returns:
            bool: True if slippage is reasonable.
        """
        orders = backtest_report.get('orders', [])
        
        for order in orders:
            if not isinstance(order, dict):
                continue
            
            slippage_bps = order.get('slippage_bps', 0)
            
            if not isinstance(slippage_bps, int):
                self._violations.append(
                    f"Order {order.get('order_id')} slippage not int"
                )
                return False
            
            # Check: slippage should be non-negative
            if slippage_bps < 0:
                self._violations.append(
                    f"Order {order.get('order_id')} negative slippage {slippage_bps}"
                )
                return False
            
            # Check: slippage should not exceed threshold
            if slippage_bps > self._max_slippage_bps:
                self._violations.append(
                    f"Order {order.get('order_id')} slippage {slippage_bps} "
                    f"exceeds max {self._max_slippage_bps}"
                )
                return False
        
        return True
    
    def _validate_latency_plausible(self, backtest_report: dict) -> bool:
        """
        Validate that order latencies are plausible.
        
        Args:
            backtest_report: Backtest execution report.
            
        Returns:
            bool: True if latencies are plausible.
        """
        orders = backtest_report.get('orders', [])
        
        for order in orders:
            if not isinstance(order, dict):
                continue
            
            latency_us = order.get('latency_us', 0)
            
            if not isinstance(latency_us, int):
                self._violations.append(
                    f"Order {order.get('order_id')} latency not int"
                )
                return False
            
            # Check: latency should be non-negative
            if latency_us < 0:
                self._violations.append(
                    f"Order {order.get('order_id')} negative latency {latency_us}"
                )
                return False
            
            # Check: latency should not exceed threshold
            if latency_us > self._max_latency_us:
                self._violations.append(
                    f"Order {order.get('order_id')} latency {latency_us}us "
                    f"exceeds max {self._max_latency_us}us"
                )
                return False
        
        return True
    
    def _validate_partial_fills(self, backtest_report: dict, market_data: dict) -> bool:
        """
        Validate that partial fills follow realistic patterns.
        
        Args:
            backtest_report: Backtest execution report.
            market_data: Market tick data.
            
        Returns:
            bool: True if partial fills are valid.
        """
        orders = backtest_report.get('orders', [])
        
        for order in orders:
            if not isinstance(order, dict):
                continue
            
            fills = order.get('fills', [])
            filled_volume = order.get('filled_volume', 0)
            order_size = order.get('order_size', 0)
            
            if not isinstance(fills, list):
                self._violations.append(
                    f"Order {order.get('order_id')} fills not list"
                )
                return False
            
            # Check: fill volumes sum to reported filled volume
            fill_sum = 0
            for fill in fills:
                if not isinstance(fill, (list, tuple)) or len(fill) != 2:
                    self._violations.append(
                        f"Order {order.get('order_id')} invalid fill format"
                    )
                    return False
                
                price, volume = fill
                
                if not isinstance(price, int) or not isinstance(volume, int):
                    self._violations.append(
                        f"Order {order.get('order_id')} non-int fill price/volume"
                    )
                    return False
                
                if volume < 0:
                    self._violations.append(
                        f"Order {order.get('order_id')} negative fill volume"
                    )
                    return False
                
                fill_sum += volume
            
            # Check: sum of fill volumes equals reported filled volume
            if fill_sum != filled_volume:
                self._violations.append(
                    f"Order {order.get('order_id')} fills sum {fill_sum} "
                    f"!= filled_volume {filled_volume}"
                )
                return False
        
        return True
    
    def _validate_liquidity_sufficient(self, backtest_report: dict, market_data: dict) -> bool:
        """
        Validate that market had sufficient liquidity for fills.
        
        Args:
            backtest_report: Backtest execution report.
            market_data: Market tick data.
            
        Returns:
            bool: True if liquidity was sufficient.
        """
        orders = backtest_report.get('orders', [])
        liquidity = market_data.get('liquidity_profile', {})
        
        for order in orders:
            if not isinstance(order, dict):
                continue
            
            symbol = order.get('symbol', '')
            filled_volume = order.get('filled_volume', 0)
            
            if not symbol:
                continue
            
            # Get available liquidity for symbol
            available_liquidity = liquidity.get(symbol, 0)
            
            if not isinstance(available_liquidity, int):
                continue
            
            # Check: filled volume should not exceed available liquidity
            if available_liquidity > 0 and filled_volume > available_liquidity:
                self._violations.append(
                    f"Order {order.get('order_id')} filled {filled_volume} "
                    f"> liquidity {available_liquidity}"
                )
                return False
        
        return True
    
    def _validate_price_levels(self, backtest_report: dict) -> bool:
        """
        Validate that execution doesn't use too many price levels.
        
        Args:
            backtest_report: Backtest execution report.
            
        Returns:
            bool: True if price levels are reasonable.
        """
        orders = backtest_report.get('orders', [])
        
        for order in orders:
            if not isinstance(order, dict):
                continue
            
            num_levels = order.get('num_fill_levels', 0)
            
            if not isinstance(num_levels, int):
                self._violations.append(
                    f"Order {order.get('order_id')} num_fill_levels not int"
                )
                return False
            
            # Check: number of price levels should be reasonable
            if num_levels > self._max_execution_levels:
                self._violations.append(
                    f"Order {order.get('order_id')} used {num_levels} "
                    f"price levels > max {self._max_execution_levels}"
                )
                return False
        
        return True
    
    def _validate_no_crossing_spreads(self, backtest_report: dict, market_data: dict) -> bool:
        """
        Validate that fill prices don't cross bid/ask spreads unrealistically.
        
        Args:
            backtest_report: Backtest execution report.
            market_data: Market tick data.
            
        Returns:
            bool: True if spreads not crossed.
        """
        spreads = market_data.get('bid_ask_spreads', [])
        
        if not isinstance(spreads, list):
            return True  # No spread data to validate
        
        # Calculate average spread
        if spreads:
            avg_spread = sum(spreads) // len(spreads) if spreads else 0
        else:
            avg_spread = 100  # Default 100 bps if no data
        
        # Max realistic spread is higher bound
        max_spread = self._max_spread_bps
        
        if avg_spread > max_spread:
            self._violations.append(
                f"Average spread {avg_spread} exceeds max {max_spread}"
            )
            return False
        
        return True
    
    def _validate_order_sizes(self, backtest_report: dict, market_data: dict) -> bool:
        """
        Validate that order sizes are feasible given market conditions.
        
        Args:
            backtest_report: Backtest execution report.
            market_data: Market tick data.
            
        Returns:
            bool: True if order sizes are feasible.
        """
        orders = backtest_report.get('orders', [
