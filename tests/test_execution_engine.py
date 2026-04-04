import unittest
from tare.execution.execution_engine import ExecutionEngine

class TestExecutionEngine(unittest.TestCase):
    def setUp(self):
        self.engine = ExecutionEngine()

    def test_init_default(self):
        self.assertEqual(self.engine.get_order_count(), 0)

    def test_init_negative_latency(self):
        with self.assertRaises(ValueError):
            ExecutionEngine(base_latency_ns=-1)

    def test_init_float_latency(self):
        with self.assertRaises(ValueError):
            ExecutionEngine(base_latency_ns=1000.5)

    def test_submit_buy(self):
        oid = self.engine.submit_order("EURUSD", "buy", 100, 1000)
        self.assertEqual(oid, 1)

    def test_submit_invalid_side(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order("EURUSD", "Buy", 100, 1000)

    def test_submit_invalid_size(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order("EURUSD", "buy", 0, 1000)

    def test_report_structure(self):
        oid = self.engine.submit_order("EURUSD", "buy", 100, 1000)
        r = self.engine.get_execution_report(oid)
        for key in ["order_id","symbol","side","requested_size",
                    "requested_price","executed_size","executed_price",
                    "latency_ns","status"]:
            self.assertIn(key, r)

    def test_report_invalid_id(self):
        with self.assertRaises(ValueError):
            self.engine.get_execution_report(999)

    def test_determinism(self):
        e1, e2 = ExecutionEngine(), ExecutionEngine()
        e1.submit_order("EURUSD", "buy", 100, 1000)
        e2.submit_order("EURUSD", "buy", 100, 1000)
        self.assertEqual(e1.get_execution_report(1), e2.get_execution_report(1))

if __name__ == "__main__":
    unittest.main()
