"""
Partial order fill simulation based on market depth.

This module provides deterministic simulation of partial order fills
using market depth data. It processes orders against a limit order book
snapshot to generate a sequence of fills at different price levels.

Phase: 2
Role: Simulate partial order fills based on market depth
"""

from typing import List, Tuple


class PartialFillSimulator:
    """
    Simulate partial fills of an order against market depth.

    This class implements a deterministic algorithm to fill an order
    of a given size against a provided market depth snapshot. It processes
    price levels in order (best bid/ask first) until the order is fully
    filled or market depth is exhausted.

    The simulation follows strict TARE rules:
    - Only integer arithmetic
    - No randomness (deterministic)
    - No external dependencies (standard library only)
    """

    def __init__(self):
        """
        Initialize the PartialFillSimulator.

        No parameters needed as the simulator is stateless.
        """
        pass

    def fill_order(self, order_size: int, market_depth: dict) -> List[Tuple[int, int]]:
        """
        Simulate filling an order against market depth.

        Processes the order through available liquidity at different price
        levels, returning a list of fills as (price, volume) tuples.

        Args:
            order_size: Total size of the order to fill (positive int).
            market_depth: Dictionary representing market depth with structure:
                {
                    'bids': [(price1, volume1), (price2, volume2), ...],
                    'asks': [(price1, volume1), (price2, volume2), ...]
                }
                Bids are sorted descending (best bid first).
                Asks are sorted ascending (best ask first).

        Returns:
            List of tuples representing fills: [(price1, volume1), (price2, volume2), ...]
            where price is the execution price and volume is the filled volume at that price.
            Returns empty list if order_size <= 0 or market depth is empty.

        Raises:
            ValueError: If order_size is not a positive integer.
            ValueError: If market_depth doesn't have required structure.
            ValueError: If price/volume in market depth are not integers.
        """
        # Validate inputs
        if not isinstance(order_size, int):
            raise ValueError("order_size must be an integer")
        if order_size == 0:
            return []

        if not isinstance(market_depth, dict):
            raise ValueError("market_depth must be a dictionary")

        if 'bids' not in market_depth or 'asks' not in market_depth:
            raise ValueError("market_depth must contain 'bids' and 'asks' keys")

        # Determine which side to use based on order sign
        # Validate market depth price/volume types
        for side in ('bids', 'asks'):
            for price, volume in market_depth.get(side, []):
                if not isinstance(price, int):
                    raise ValueError(f"Price {price} must be an integer")
                if not isinstance(volume, int):
                    raise ValueError(f"Volume {volume} must be an integer")

        # Positive order_size indicates buy order (fills from asks)
        # Negative order_size indicates sell order (fills from bids)
        if order_size > 0:
            side = 'asks'
            remaining = order_size
            levels = market_depth[side]
        else:
            side = 'bids'
            remaining = -order_size  # Convert to positive for processing
            levels = market_depth[side]

        # Validate market depth structure
        if not isinstance(levels, list):
            raise ValueError(f"market_depth['{side}'] must be a list")

        fills = []

        # Process each price level until order is filled or depth exhausted
        for price, volume in levels:
            # Validate price and volume are integers
            if not isinstance(price, int):
                raise ValueError(f"Price {price} must be an integer")
            if not isinstance(volume, int):
                raise ValueError(f"Volume {volume} must be an integer")

            if volume <= 0:
                continue  # Skip empty levels

            if remaining <= 0:
                break  # Order already filled

            # Determine fill volume at this price level
            fill_volume = min(remaining, volume)

            # Add to fills list
            fills.append((price, fill_volume))

            # Update remaining order size
            remaining -= fill_volume

        # If we have a buy order, return fills as-is
        # If we have a sell order, we need to return negative volumes
        # to indicate sell execution
        if order_size > 0:
            return fills
        else:
            # For sell orders, return fills with negative volumes
            return [(price, -volume) for price, volume in fills]

    def fill_order_with_slippage(self, order_size: int, market_depth: dict) -> List[Tuple[int, int]]:
        """
        Simulate order fills with explicit slippage calculation.

        This is a convenience method that returns the same result as fill_order
        but provides additional context about the execution.

        Args:
            order_size: Total size of the order to fill (positive int for buy,
                       negative int for sell).
            market_depth: Dictionary representing market depth with 'bids' and 'asks'.

        Returns:
            List of tuples representing fills: [(price1, volume1), (price2, volume2), ...]

        Raises:
            ValueError: If inputs are invalid.
        """
        return self.fill_order(order_size, market_depth)

    def calculate_execution_price(self, fills: List[Tuple[int, int]]) -> int:
        """
        Calculate volume-weighted average execution price from fills.

        Args:
            fills: List of fills as returned by fill_order.

        Returns:
            Volume-weighted average price as integer (rounded down).
            Returns 0 if fills list is empty.

        Raises:
            ValueError: If fills contain non-integer values.
        """
        if not fills:
            return 0

        total_volume = 0
        total_value = 0

        for price, volume in fills:
            if not isinstance(price, int):
                raise ValueError(f"Price {price} must be an integer")
            if not isinstance(volume, int):
                raise ValueError(f"Volume {volume} must be an integer")

            # Use absolute volume for calculation
            abs_volume = abs(volume)
            total_volume += abs_volume
            total_value += price * abs_volume

        if total_volume == 0:
            return 0

        # Integer division for deterministic result
        return total_value // total_volume

    def calculate_slippage(self, fills: List[Tuple[int, int]], reference_price: int) -> int:
        """
        Calculate slippage relative to reference price.

        Slippage is defined as the difference between volume-weighted
        average execution price and reference price.

        Args:
            fills: List of fills as returned by fill_order.
            reference_price: Reference price to compare against.

        Returns:
            Slippage as integer (execution_price - reference_price).
            Returns 0 if fills list is empty.

        Raises:
            ValueError: If reference_price is not an integer.
        """
        if not isinstance(reference_price, int):
            raise ValueError("reference_price must be an integer")

        if not fills:
            return 0

        execution_price = self.calculate_execution_price(fills)
        return execution_price - reference_price

    def get_unfilled_amount(self, order_size: int, fills: List[Tuple[int, int]]) -> int:
        """
        Calculate unfilled portion of the original order.

        Args:
            order_size: Original order size (positive for buy, negative for sell).
            fills: List of fills as returned by fill_order.

        Returns:
            Remaining unfilled amount (positive for buy, negative for sell).
            If order_size > 0 and fills sum to less than order_size,
            returns positive remainder. If order_size < 0 and fills sum
            to more than order_size, returns negative remainder.

        Raises:
            ValueError: If order_size is not an integer.
        """
        if not isinstance(order_size, int):
            raise ValueError("order_size must be an integer")

        if not fills:
            return order_size

        # Calculate total filled volume
        total_filled = sum(volume for _, volume in fills)

        # Return remaining amount
        return order_size - total_filled
