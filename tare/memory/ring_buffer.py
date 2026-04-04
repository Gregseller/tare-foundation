"""
Ring buffer implementation for efficient circular storage of tick data.

Provides O(1) push/pop operations with fixed memory footprint.
"""


class RingBuffer:
    """Memory-efficient circular buffer for tick streams."""

    def __init__(self, capacity: int) -> None:
        """
        Initialize ring buffer with fixed capacity.

        Args:
            capacity: Maximum number of items the buffer can hold.

        Raises:
            ValueError: If capacity is less than 1.
        """
        if capacity < 1:
            raise ValueError("Capacity must be at least 1")
        
        self._capacity: int = capacity
        self._buffer: list = [None] * capacity
        self._head: int = 0
        self._tail: int = 0
        self._size: int = 0

    def push(self, item: dict) -> None:
        """
        Add item to the buffer, overwriting oldest if full.

        Args:
            item: Dictionary to push into buffer.
        """
        if item is None:
            raise ValueError("Cannot push None item")
        
        self._buffer[self._tail] = item
        self._tail = (self._tail + 1) % self._capacity
        
        if self._size < self._capacity:
            self._size += 1
        else:
            self._head = (self._head + 1) % self._capacity

    def pop(self) -> dict:
        """
        Remove and return oldest item from buffer.

        Returns:
            Dictionary at the front of the buffer.

        Raises:
            IndexError: If buffer is empty.
        """
        if self._size == 0:
            raise IndexError("Cannot pop from empty buffer")
        
        item = self._buffer[self._head]
        self._buffer[self._head] = None
        self._head = (self._head + 1) % self._capacity
        self._size -= 1
        
        return item

    def peek(self, offset: int) -> dict:
        """
        View item at given offset without removing it.

        Args:
            offset: Position from head (0 = oldest, size-1 = newest).

        Returns:
            Dictionary at specified offset.

        Raises:
            IndexError: If offset is out of bounds.
            ValueError: If offset is negative.
        """
        if offset < 0:
            raise ValueError("Offset must be non-negative")
        
        if offset >= self._size:
            raise IndexError(f"Offset {offset} out of bounds for size {self._size}")
        
        index = (self._head + offset) % self._capacity
        return self._buffer[index]

    def is_full(self) -> bool:
        """
        Check if buffer is at maximum capacity.

        Returns:
            True if buffer contains capacity items, False otherwise.
        """
        return self._size == self._capacity

    def __len__(self) -> int:
        """Return current number of items in buffer."""
        return self._size

    def __repr__(self) -> str:
        """Return string representation of buffer state."""
        return f"RingBuffer(capacity={self._capacity}, size={self._size})"
