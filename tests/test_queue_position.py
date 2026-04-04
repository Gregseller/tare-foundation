import unittest
from tare.microstructure.queue_position import QueuePosition


class TestQueuePosition(unittest.TestCase):
    def setUp(self):
        self.qp = QueuePosition()

    def test_estimate_queue_ahead_normal(self):
        depth = [(100, 50), (99, 30), (98, 20)]
        result = self.qp.estimate_queue_ahead(10, depth)
        self.assertIsInstance(result, int)
        self.assertEqual(result, 50 + 30 + 20)

    def test_estimate_queue_ahead_order_size_zero(self):
        depth = [(100, 50), (99, 30)]
        result = self.qp.estimate_queue_ahead(0, depth)
        self.assertEqual(result, 0)

    def test_estimate_queue_ahead_negative_order_size(self):
        depth = [(100, 50)]
        result = self.qp.estimate_queue_ahead(-5, depth)
        self.assertEqual(result, 0)

    def test_estimate_queue_ahead_depth_not_list(self):
        result = self.qp.estimate_queue_ahead(10, "not a list")
        self.assertEqual(result, 0)

    def test_estimate_queue_ahead_empty_depth(self):
        result = self.qp.estimate_queue_ahead(10, [])
        self.assertEqual(result, 0)

    def test_estimate_queue_ahead_skips_invalid_price(self):
        depth = [(100, 50), (99.5, 30), (98, 20)]
        result = self.qp.estimate_queue_ahead(10, depth)
        self.assertEqual(result, 50 + 20)  # 99.5 skipped

    def test_estimate_queue_ahead_skips_invalid_volume(self):
        depth = [(100, 50), (99, "30"), (98, 20)]
        result = self.qp.estimate_queue_ahead(10, depth)
        self.assertEqual(result, 50 + 20)

    def test_estimate_queue_ahead_skips_zero_volume(self):
        depth = [(100, 50), (99, 0), (98, 20)]
        result = self.qp.estimate_queue_ahead(10, depth)
        self.assertEqual(result, 50 + 20)

    def test_estimate_queue_ahead_large_numbers(self):
        depth = [(100, 2**62), (99, 2**62)]
        result = self.qp.estimate_queue_ahead(1, depth)
        self.assertEqual(result, 2**63 - 1)  # capped

    def test_estimate_fill_time_normal(self):
        result = self.qp.estimate_fill_time(5000, 1000)
        self.assertIsInstance(result, int)
        self.assertEqual(result, 5000 // 1000)

    def test_estimate_fill_time_queue_ahead_zero(self):
        result = self.qp.estimate_fill_time(0, 1000)
        self.assertEqual(result, 0)

    def test_estimate_fill_time_tick_rate_zero(self):
        result = self.qp.estimate_fill_time(5000, 0)
        self.assertEqual(result, 0)

    def test_estimate_fill_time_negative_queue_ahead(self):
        result = self.qp.estimate_fill_time(-100, 1000)
        self.assertEqual(result, 0)

    def test_estimate_fill_time_negative_tick_rate(self):
        result = self.qp.estimate_fill_time(100, -10)
        self.assertEqual(result, 0)

    def test_estimate_fill_time_non_int_inputs(self):
        result = self.qp.estimate_fill_time(100.5, 10)
        self.assertEqual(result, 0)
        result2 = self.qp.estimate_fill_time(100, 10.5)
        self.assertEqual(result2, 0)

    def test_determinism(self):
        depth = [(100, 50), (99, 30)]
        order_size = 10
        r1 = self.qp.estimate_queue_ahead(order_size, depth)
        r2 = self.qp.estimate_queue_ahead(order_size, depth)
        self.assertEqual(r1, r2)

        qa = 5000
        rate = 1000
        t1 = self.qp.estimate_fill_time(qa, rate)
        t2 = self.qp.estimate_fill_time(qa, rate)
        self.assertEqual(t1, t2)

    def test_int_return_type_always(self):
        # Various inputs that should return int
        cases = [
            (self.qp.estimate_queue_ahead, (10, [(100, 50)])),
            (self.qp.estimate_queue_ahead, (0, [(100, 50)])),
            (self.qp.estimate_queue_ahead, (10, [])),
            (self.qp.estimate_queue_ahead, (10, "bad")),
            (self.qp.estimate_fill_time, (100, 10)),
            (self.qp.estimate_fill_time, (0, 10)),
            (self.qp.estimate_fill_time, (100, 0)),
            (self.qp.estimate_fill_time, (100.5, 10)),
        ]
        for func, args in cases:
            result = func(*args)
            self.assertIsInstance(result, int, f"{func.__name__}{args} returned {type(result)}")


if __name__ == "__main__":
    unittest.main()