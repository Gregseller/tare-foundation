"""
portfolio_engine.py — Portfolio engine for TARE.
Phase: 3
"""

class PortfolioEngine:
    """Track positions and P&L for a portfolio."""

    def __init__(self):
        self._positions = {}      # symbol -> position (int)
        self._avg_prices = {}     # symbol -> average entry price (int)
        self._transaction_count = 0

    def update_position(self, symbol: str, side: str, size: int, price: int) -> None:
        if not isinstance(symbol, str):
            raise ValueError("symbol must be str")
        if side not in ("buy", "sell"):
            raise ValueError("side must be 'buy' or 'sell'")
        if not isinstance(size, int) or size <= 0:
            raise ValueError("size must be positive int")
        if not isinstance(price, int) or price <= 0:
            raise ValueError("price must be positive int")

        old_pos = self._positions.get(symbol, 0)

        if side == "buy":
            new_pos = old_pos + size
            # Calculate new average price: (old_pos * avg_price + size * price) / new_pos
            if new_pos == 0:
                new_avg = 0
            else:
                old_avg = self._avg_prices.get(symbol, 0)
                total_value = old_pos * old_avg + size * price
                new_avg = total_value // new_pos
            self._positions[symbol] = new_pos
            self._avg_prices[symbol] = new_avg
        else:  # sell
            new_pos = old_pos - size
            if new_pos < 0:
                raise ValueError("sell would create negative position (short not allowed)")
            self._positions[symbol] = new_pos
            # Average price remains unchanged after sell
            if new_pos == 0:
                self._avg_prices.pop(symbol, None)
            # else keep same avg price

        self._transaction_count += 1

    def get_position(self, symbol: str) -> int:
        if not isinstance(symbol, str):
            raise ValueError("symbol must be str")
        return self._positions.get(symbol, 0)

    def get_pnl(self, symbol: str, current_price: int) -> int:
        if not isinstance(symbol, str):
            raise ValueError("symbol must be str")
        if not isinstance(current_price, int) or current_price <= 0:
            raise ValueError("current_price must be positive int")

        pos = self._positions.get(symbol, 0)
        if pos == 0:
            return 0

        avg_price = self._avg_prices.get(symbol, 0)
        if avg_price == 0:
            return 0

        # P&L = position * (current_price - avg_price)
        return pos * (current_price - avg_price)

    def get_transaction_count(self) -> int:
        return self._transaction_count