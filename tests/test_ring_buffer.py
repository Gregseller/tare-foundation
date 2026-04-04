"""
Unit tests for RingBuffer module.
"""

import unittest
from tare.memory.ring_buffer import RingBuffer


class TestRingBufferInit(unittest.TestCase):
    """Tests for RingBuffer initialization."""

    def test_init_valid_capacity(self):
        """Test initialization with valid capacity."""
        rb = RingBuffer(5)
        self.assertEqual(len(rb), 0)
        self.assertFalse(rb.is_full())

    def test_init_capacity_one(self):
        """Test initialization with minimum capacity."""
        rb = RingBuffer(1)
        self.assertEqual(len(rb), 0)

    def test_init_invalid_capacity_zero(self):
        """Test initialization with zero capacity raises error."""
        with self.assertRaises(ValueError):
            RingBuffer(0)

    def test_init_invalid_capacity_negative(self):
        """Test initialization with negative capacity raises error."""
        with self.assertRaises(ValueError):
            RingBuffer(-5)


class TestRingBufferPush(unittest.TestCase):
    """Tests for push operation."""

    def setUp(self):
        """Create test buffer."""
        self.rb = RingBuffer(3)

    def test_push_single_item(self):
        """Test pushing single item."""
        item = {"tick": 100, "price": 150}
        self.rb.push(item)
        self.assertEqual(len(self.rb), 1)
        self.assertFalse(self.rb.is_full())

    def test_push_fill_buffer(self):
        """Test pushing items until full."""
        items = [{"id": 0}, {"id": 1}, {"id": 2}]
        for item in items:
            self.rb.push(item)
        self.assertEqual(len(self.rb), 3)
        self.assertTrue(self.rb.is_full())

    def test_push_none_raises_error(self):
        """Test pushing None raises error."""
        with self.assertRaises(ValueError):
            self.rb.push(None)

    def test_push_overwrites_on_full(self):
        """Test push overwrites oldest when buffer full."""
        self.rb.push({"id": 0})
        self.rb.push({"id": 1})
        self.rb.push({"id": 2})
        self.rb.push({"id": 3})
        
        self.assertEqual(len(self.rb), 3)
        self.assertEqual(self.rb.peek(0)["id"], 1)
        self.assertEqual(self.rb.peek(1)["id"], 2)
        self.assertEqual(self.rb.peek(2)["id"], 3)

    def test_push_deterministic(self):
        """Test that push operations are deterministic."""
        items = [{"val": i} for i in range(5)]
        
        rb1 = RingBuffer(3)
        rb2 = RingBuffer(3)
        
        for item in items:
            rb1.push(item)
            rb2.push(item)
        
        for i in range(3):
            self.assertEqual(rb1.peek(i), rb2.peek(i))


class TestRingBufferPop(unittest.TestCase):
    """Tests for pop operation."""

    def setUp(self):
        """Create test buffer and populate it."""
        self.rb = RingBuffer(3)
        self.rb.push({"id": 0})
        self.rb.push({"id": 1})
        self.rb.push({"id": 2})

    def test_pop_returns_oldest(self):
        """Test pop returns oldest item."""
        item = self.rb.pop()
        self.assertEqual(item["id"], 0)
        self.assertEqual(len(self.rb), 2)

    def test_pop_sequence(self):
        """Test sequence of pops."""
        item0 = self.rb.pop()
        item1 = self.rb.pop()
        item2 = self.rb.pop()
        
        self.assertEqual(item0["id"], 0)
        self.assertEqual(item1["id"], 1)
        self.assertEqual(item2["id"], 2)
        self.assertEqual(len(self.rb), 0)

    def test_pop_empty_raises_error(self):
        """Test pop from empty buffer raises error."""
        self.rb.pop()
        self.rb.pop()
        self.rb.pop()
        
        with self.assertRaises(IndexError):
            self.rb.pop()

    def test_pop_after_overwrite(self):
        """Test pop after wraparound overwrites."""
        self.rb.push({"id": 3})
        item = self.rb.pop()
        self.assertEqual(item["id"], 1)


class TestRingBufferPeek(unittest.TestCase):
    """Tests for peek operation."""

    def setUp(self):
        """Create test buffer and populate it."""
        self.rb = RingBuffer(4)
        self.rb.push({"id": 10})
        self.rb.push({"id": 20})
        self.rb.push({"id": 30})

    def test_peek_head(self):
        """Test peek at offset 0 returns oldest."""
        item = self.rb.peek(0)
        self.assertEqual(item["id"], 10)

    def test_peek_tail(self):
        """Test peek at last offset returns newest."""
        item = self.rb.peek(2)
        self.assertEqual(item["id"], 30)

    def test_peek_middle(self):
        """Test peek at middle offset."""
        item = self.rb.peek(1)
        self.assertEqual(item["id"], 20)

    def test_peek_negative_offset_raises_error(self):
        """Test negative offset raises error."""
        with self.assertRaises(ValueError):
            self.rb.peek(-1)

    def test_peek_out_of_bounds_raises_error(self):
        """Test offset beyond size raises error."""
        with self.assertRaises(IndexError):
            self.rb.peek(5)

    def test_peek_does_not_modify_buffer(self):
        """Test peek does not change buffer state."""
        self.rb.peek(0)
        self.rb.peek(1)
        self.assertEqual(len(self.rb), 3)
        self.assertEqual(self.rb.peek(0)["id"], 10)

    def test_peek_after_wraparound(self):
        """Test peek works correctly after wraparound."""
        self.rb.push({"id": 40})
        self.rb.push({"id": 50})
        
        self.assertEqual(self.rb.peek(0)["id"], 20)
        self.assertEqual(self.rb.peek(1)["id"], 30)
        self.assertEqual(self.rb.peek(2)["id"], 40)
        self.assertEqual(self.rb.peek(3)["id"], 50)


class TestRingBufferIsFull(unittest.TestCase):
    """Tests for is_full operation."""

    def test_is_full_empty_buffer(self):
        """Test is_full on empty buffer."""
        rb = RingBuffer(3)
        self.assertFalse(rb.is_full())

    def test_is_full_partial_buffer(self):
        """Test is_full on partially filled buffer."""
        rb = RingBuffer(3)
        rb.push({"id": 0})
        rb.push({"id": 1})
        self.assertFalse(rb.is_full())

    def test_is_full_complete_buffer(self):
        """Test is_full on full buffer."""
        rb = RingBuffer(3)
        rb.push({"id": 0})
        rb.push({"id": 1})
        rb.push({"id": 2})
        self.assertTrue(rb.is_full())

    def test_is_full_after_pop(self):
        """Test is_full after popping from full buffer."""
        rb = RingBuffer(3)
        rb.push({"id": 0})
        rb.push({"id": 1})
        rb.push({"id": 2})
        rb.pop()
        self.assertFalse(rb.is_full())

    def test_is_full_capacity_one(self):
        """Test is_full with capacity 1."""
        rb = RingBuffer(1)
        self.assertFalse(rb.is_full())
        rb.push({"id": 0})
        self.assertTrue(rb.is_full())


class TestRingBufferIntegration(unittest.TestCase):
    """Integration tests combining multiple operations."""

    def test_push_pop_cycle(self):
        """Test repeated push-pop cycles."""
        rb = RingBuffer(2)
        
        for cycle in range(3):
            rb.push({"cycle": cycle, "val": 0})
            rb.push({"cycle": cycle, "val": 1})
            
            item0 = rb.pop()
            item1 = rb.pop()
            
            self.assertEqual(item0["val"], 0)
            self.assertEqual(item1["val"], 1)

    def test_mixed_operations(self):
        """Test mixed push, pop, and peek operations."""
        rb = RingBuffer(4)
        
        rb.push({"id": 1})
        rb.push({"id": 2})
        self.assertEqual(rb.peek(0)["id"], 1)
        
        rb.push({"id": 3})
        rb.pop()
        self.assertEqual(rb.peek(0)["id"], 2)
        
        rb.push({"id": 4})
        rb.push({"id": 5})
        self.assertTrue(rb.is_full())
        self.assertEqual(len(rb), 4)

    def test_large_capacity_wraparound(self):
        """Test buffer with larger capacity and multiple wraparounds."""
        rb = RingBuffer(100)
        
        for i in range(250):
            rb.push({"seq": i})
        
        self.assertEqual(len(rb), 100)
        self.assertTrue(rb.is_full())
        self.assertEqual(rb.peek(0)["seq"], 150)
        self.assertEqual(rb.peek(99)["seq"], 249)


if __name__ == "__main__":
    unittest.main()
