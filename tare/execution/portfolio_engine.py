"""
portfolio_engine.py — PortfolioEngine v1
TARE (Tick-Level Algorithmic Research Environment)

Track positions, P&L, exposure, risk metrics.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Детерминизм: одни входные данные → всегда одинаковый результат
  - Только стандартная библиотека Python
"""

from typing import Generator, Optional


class PortfolioEngine:
    """
    Track positions, P&L, exposure, and risk metrics across portfolio.
    
    Maintains live portfolio state with position tracking, unrealized/realized P&L,
    exposure calculations, and risk metric computation. All arithmetic uses
    integer basis points for deterministic calculations.
    """
    
    def __init__(self, execution_engine, tick_data_engine):
        """
        Initialize PortfolioEngine with dependencies.
        
        Args:
            execution_engine: ExecutionEngine instance for order execution data.
            tick_data_engine: TickDataEngine instance for market tick data.
            
        Raises:
            ValueError: If any dependency is None.
        """
        if execution_engine is None:
            raise ValueError("execution_engine cannot be None")
        if tick_data_engine is None:
            raise ValueError("tick_data_engine cannot be None")
        
        self._execution_engine = execution_engine
        self._tick_data_engine = tick_data_engine
        
        # Position tracking: symbol -> {'quantity': int, 'cost_basis': int, 'fills': list}
        self._positions = {}
        
        # Market prices in basis points: symbol -> int
        self._market_prices = {}
        
        # Trade history: list of {'symbol': str, 'quantity': int, 'price': int, 'side': str}
        self._trade_history = []
        
        # Realized P&L tracking: symbol -> int (in basis points)
        self._realized_pnl = {}
        
        # Risk metrics cache
        self._risk_cache = {
            'gross_exposure': 0,
            'net_exposure': 0,
            'leverage': 0,
            'var_95': 0,
            'max_drawdown': 0
        }
        
        self._pnl_history = []  # List of daily P&L snapshots
    
    def add_position(self, symbol: str, quantity: int, price: int) -> None:
        """
        Add or update a position in the portfolio.
        
        Records a position entry at specified price. Quantity can be positive (long)
        or negative (short). Updates cost basis and tracks fills for P&L calculation.
        All prices are in basis points (no decimal). Deterministic operation.
        
        Args:
            symbol: Asset symbol (uppercase string, e.g., 'EURUSD').
            quantity: Position size in units (positive for long, negative for short).
            price: Entry price in basis points (integer, e.g., 10000 = 1.00 at 4 decimals).
            
        Raises:
            ValueError: If parameters are invalid.
            TypeError: If parameters have wrong types.
        """
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol must be non-empty string")
        
        if not isinstance(quantity, int):
            raise ValueError("quantity must be an integer")
        
        if not isinstance(price, int) or price < 0:
            raise ValueError("price must be a non-negative integer")
        
        symbol = symbol.upper()
        
        # Initialize position if not exists
        if symbol not in self._positions:
            self._positions[symbol] = {
                'quantity': 0,
                'cost_basis': 0,
                'fills': [],
                'entry_price': 0
            }
            self._realized_pnl[symbol] = 0
        
        pos = self._positions[symbol]
        
        # Update quantity and cost basis
        old_quantity = pos['quantity']
        new_quantity = old_quantity + quantity
        
        # Calculate weighted average cost basis
        if new_quantity != 0:
            if old_quantity == 0:
                # New position
                pos['entry_price'] = price
                pos['cost_basis'] = price * quantity
            else:
                # Add to existing position
                total_cost = pos['cost_basis'] + (price * quantity)
                pos['entry_price'] = total_cost // new_quantity if new_quantity != 0 else price
                pos['cost_basis'] = total_cost
        else:
            # Position closed
            pos['entry_price'] = 0
            pos['cost_basis'] = 0
        
        pos['quantity'] = new_quantity
        
        # Record fill
        pos['fills'].append({
            'quantity': quantity,
            'price': price,
            'timestamp': len(self._trade_history)  # Deterministic timestamp
        })
        
        # Track trade in history
        self._trade_history.append({
            'symbol': symbol,
            'quantity': quantity,
            'price': price,
            'side': 'BUY' if quantity > 0 else 'SELL',
            'trade_id': len(self._trade_history)
        })
        
        # Update market price
        if symbol not in self._market_prices:
            self._market_prices[symbol] = price
    
    def update_market_price(self, symbol: str, price: int) -> None:
        """
        Update current market price for a symbol.
        
        Updates the market price used for unrealized P&L and exposure calculations.
        Price must be non-negative integer in basis points.
        
        Args:
            symbol: Asset symbol.
            price: Current market price in basis points.
            
        Raises:
            ValueError: If parameters are invalid.
        """
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol must be non-empty string")
        
        if not isinstance(price, int) or price < 0:
            raise ValueError("price must be a non-negative integer")
        
        symbol = symbol.upper()
        self._market_prices[symbol] = price
    
    def close_position(self, symbol: str, exit_price: int) -> int:
        """
        Close an entire position at exit price.
        
        Records position closure and calculates realized P&L. Returns the realized
        gain/loss in basis points. Position quantity becomes zero.
        
        Args:
            symbol: Asset symbol.
            exit_price: Exit price in basis points.
            
        Returns:
            int: Realized P&L in basis points.
            
        Raises:
            ValueError: If symbol not in portfolio or price invalid.
        """
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol must be non-empty string")
        
        if not isinstance(exit_price, int) or exit_price < 0:
            raise ValueError("exit_price must be a non-negative integer")
        
        symbol = symbol.upper()
        
        if symbol not in self._positions:
            raise ValueError(f"No position for {symbol}")
        
        pos = self._positions[symbol]
        quantity = pos['quantity']
        
        if quantity == 0:
            raise ValueError(f"Position for {symbol} is already closed")
        
        # Calculate realized P&L
        entry_price = pos['entry_price']
        pnl = (exit_price - entry_price) * quantity
        
        # Track realized P&L
        self._realized_pnl[symbol] += pnl
        
        # Record closing trade
        self._trade_history.append({
            'symbol': symbol,
            'quantity': -quantity,
            'price': exit_price,
            'side': 'SELL' if quantity > 0 else 'BUY',
            'trade_id': len(self._trade_history),
            'closed_pnl': pnl
        })
        
        # Close position
        pos['quantity'] = 0
        pos['cost_basis'] = 0
        pos['entry_price'] = 0
        
        # Update market price
        self._market_prices[symbol] = exit_price
        
        return pnl
    
    def get_pnl(self) -> dict:
        """
        Calculate current portfolio P&L breakdown.
        
        Returns comprehensive P&L report with realized, unrealized, and total P&L
        for entire portfolio and per-symbol. All values in basis points.
        Deterministic based on current positions and market prices.
        
        Returns:
            dict with keys:
                - 'total_pnl' (int): Total P&L (realized + unrealized)
                - 'realized_pnl' (int): Sum of closed positions P&L
                - 'unrealized_pnl' (int): Sum of open positions P&L
                - 'by_symbol' (dict): {symbol: {'realized': int, 'unrealized': int, 'total': int}}
                
        Example:
            {
                'total_pnl': 5000,
                'realized_pnl': 2000,
                'unrealized_pnl': 3000,
                'by_symbol': {
                    'EURUSD': {'realized': 1000, 'unrealized': 2000, 'total': 3000},
                    'GBPUSD': {'realized': 1000, 'unrealized': 1000, 'total': 2000}
                }
            }
        """
        pnl_report = {
            'total_pnl': 0,
            'realized_pnl': 0,
            'unrealized_pnl': 0,
            'by_symbol': {}
        }
        
        total_realized = 0
        total_unrealized = 0
        
        for symbol, pos in self._positions.items():
            quantity = pos['quantity']
            entry_price = pos['entry_price']
            market_price = self._market_prices.get(symbol, entry_price)
            
            # Realized P&L
            realized = self._realized_pnl.get(symbol, 0)
            
            # Unrealized P&L (for open positions)
            unrealized = 0
            if quantity != 0:
                unrealized = (market_price - entry_price) * quantity
            
            total_realized += realized
            total_unrealized += unrealized
            
            pnl_report['by_symbol'][symbol] = {
                'realized': realized,
                'unrealized': unrealized,
                'total': realized + unrealized
            }
        
        pnl_report['realized_pnl'] = total_realized
        pnl_report['unrealized_pnl'] = total_unrealized
        pnl_report['total_pnl'] = total_realized + total_unrealized
        
        return pnl_report
    
    def get_exposure(self) -> dict:
        """
        Calculate portfolio exposure metrics.
        
        Computes gross notional exposure, net exposure, long/short breakdown,
        and per-symbol exposure. All values in basis points of notional exposure.
        Deterministic based on current positions and market prices.
        
        Returns:
            dict with keys:
                - 'gross_exposure' (int): Sum of absolute position values
                - 'net_exposure' (int): Algebraic sum of position values
                - 'long_exposure' (int): Sum of long position values
                - 'short_exposure' (int): Sum of short position values (negative)
                - 'by_symbol' (dict): {symbol: {'quantity': int, 'value': int, 'side': str}}
                
        Example:
            {
                'gross_exposure': 2000000,
                'net_exposure': 500000,
                'long_exposure': 1250000,
                'short_exposure': -750000,
                'by_symbol': {
                    'EURUSD': {'quantity': 100, 'value': 1000000, 'side': 'LONG'}
                }
            }
        """
        exposure_report = {
            'gross_exposure': 0,
            'net_exposure': 0,
            'long_exposure': 0,
            'short_exposure': 0,
            'by_symbol': {}
        }
        
        gross = 0
        net = 0
        long_exp = 0
        short_exp = 0
        
        for symbol, pos in self._positions.items():
            quantity = pos['quantity']
            market_price = self._market_prices.get(symbol, pos['entry_price'])
            
            if quantity == 0:
                continue
            
            # Notional value
            value = quantity * market_price
            
            gross += abs(value)
            net += value
            
            if quantity > 0:
                long_exp += value
                side = 'LONG'
            else:
                short_exp += value
                side = 'SHORT'
            
            exposure_report['by_symbol'][symbol] = {
                'quantity': quantity,
                'value': value,
                'side': side
            }
        
        exposure_report['gross_exposure'] = gross
        exposure_report['net_exposure'] = net
        exposure_report['long_exposure'] = long_exp
        exposure_report['short_exposure'] = short_exp
        
        return exposure_report
    
    def get_risk_metrics(self) -> dict:
        """
        Calculate portfolio risk metrics.
        
        Computes Value-at-Risk (95%), Sharpe ratio approximation, maximum
        drawdown, volatility estimate, and concentration metrics.
        All calculations use integer basis point arithmetic.
        Deterministic based on position history and market prices.
        
        Returns:
            dict with keys:
                - 'var_95' (int): Value-at-Risk at 95% confidence (basis points)
                - 'max_drawdown' (int): Maximum observed drawdown (basis points)
                - 'volatility' (int): Estimated volatility (basis points)
                - 'sharpe_ratio' (int): Sharpe ratio approximation (scaled by 1000)
                - 'concentration' (int): Herfindahl concentration index (0-10000)
                - 'num_positions' (int): Number of open positions
                - 'largest_position' (dict): {'symbol': str, 'size': int, 'value': int}
                
        Example:
            {
                'var_95': 50000,
                'max_drawdown': 100000,
                'volatility': 25000,
                'sharpe_ratio': 1500,
                'concentration': 3500,
                'num_positions': 5,
                'largest_position': {'symbol': 'EURUSD', 'size': 1000, 'value': 10000000}
            }
        """
        risk_report = {
            'var_95': 0,
            'max_drawdown': 0,
            'volatility': 0,
            'sharpe_ratio': 0,
            'concentration': 0,
            'num_positions': 0,
            'largest_position': None
        }
        
        # Count open positions
        open_positions = 0
        for symbol, pos in self._positions.items():
            if pos['quantity'] != 0:
                open_positions += 1
        
        risk_report['num_positions'] = open_positions
        
        # Calculate exposure for concentration
        exposure = self.get_exposure()
        gross_exp = exposure['gross_exposure']
        
        # Calculate concentration (Herfindahl index)
        concentration = 0
        largest_pos = None
        largest_value = 0
        
        for symbol, sym_exp in exposure['by_symbol'].items():
            value = abs(sym_exp['value'])
            if gross_exp > 0:
                weight = (value * 10000) // gross_exp  # In basis points
                concentration += (weight * weight) // 10000
            
            if value
