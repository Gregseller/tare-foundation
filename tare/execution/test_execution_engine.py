import unittest
from tare.execution.execution_engine import ExecutionEngine


class TestExecutionEngine(unittest.TestCase):
    def setUp(self):
        self.engine = ExecutionEngine()
        self.engine2 = ExecutionEngine(base_latency_ns=2000, slippage_factor=2)

    # ----- __init__ -----
    def test_init_default(self):
        engine = ExecutionEngine()
        self.assertIsInstance(engine, ExecutionEngine)
        self.assertEqual(engine.get_order_count(), 0)

    def test_init_custom(self):
        engine = ExecutionEngine(base_latency_ns=5000, slippage_factor=3)
        self.assertEqual(engine.get_order_count(), 0)

    def test_init_negative_latency(self):
        with self.assertRaises(ValueError):
            ExecutionEngine(base_latency_ns=-1)

    def test_init_negative_factor(self):
        with self.assertRaises(ValueError):
            ExecutionEngine(slippage_factor=-1)

    def test_init_float_latency(self):
        with self.assertRaises(ValueError):
            ExecutionEngine(base_latency_ns=1000.5)

    def test_init_float_factor(self):
        with self.assertRaises(ValueError):
            ExecutionEngine(slippage_factor=1.5)

    # ----- submit_order -----
    def test_submit_order_buy(self):
        order_id = self.engine.submit_order("EURUSD", "buy", 10000, 100000)
        self.assertIsInstance(order_id, int)
        self.assertEqual(order_id, 1)
        self.assertEqual(self.engine.get_order_count(), 1)

    def test_submit_order_sell(self):
        order_id = self.engine.submit_order("GBPUSD", "sell", 5000, 150000)
        self.assertEqual(order_id, 2)
        self.assertEqual(self.engine.get_order_count(), 2)

    def test_submit_order_invalid_symbol_type(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order(123, "buy", 100, 1000)

    def test_submit_order_invalid_side(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order("EURUSD", "invalid", 100, 1000)

    def test_submit_order_side_case_sensitive(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order("EURUSD", "Buy", 100, 1000)

    def test_submit_order_size_zero(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order("EURUSD", "buy", 0, 1000)

    def test_submit_order_size_negative(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order("EURUSD", "buy", -100, 1000)

    def test_submit_order_size_float(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order("EURUSD", "buy", 100.5, 1000)

    def test_submit_order_price_zero(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order("EURUSD", "buy", 100, 0)

    def test_submit_order_price_negative(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order("EURUSD", "buy", 100, -100)

    def test_submit_order_price_float(self):
        with self.assertRaises(ValueError):
            self.engine.submit_order("EURUSD", "buy", 100, 1000.5)

    def test_submit_order_sequential_ids(self):
        id1 = self.engine.submit_order("A", "buy", 10, 100)
        id2 = self.engine.submit_order("B", "sell", 20, 200)
        id3 = self.engine.submit_order("C", "buy", 30, 300)
        self.assertEqual(id1, 1)
        self.assertEqual(id2, 2)
        self.assertEqual(id3, 3)

    # ----- get_execution_report -----
    def test_get_execution_report_structure(self):
        order_id = self.engine.submit_order("EURUSD", "buy", 10000, 100000)
        report = self.engine.get_execution_report(order_id)
        required_keys = {"order_id", "symbol", "side", "requested_size",
                         "requested_price", "executed_size", "executed_price",
                         "latency_ns", "status"}
        self.assertEqual(set(report.keys()), required_keys)
        self.assertIsInstance(report["order_id"], int)
        self.assertIsInstance(report["symbol"], str)
        self.assertIsInstance(report["side"], str)
        self.assertIsInstance(report["requested_size"], int)
        self.assertIsInstance(report["requested_price"], int)
        self.assertIsInstance(report["executed_size"], int)
        self.assertIsInstance(report["executed_price"], int)
        self.assertIsInstance(report["latency_ns"], int)
        self.assertIsInstance(report["status"], str)

    def test_get_execution_report_values(self):
        order_id = self.engine.submit_order("GBPUSD", "sell", 5000, 150000)
        report = self.engine.get_execution_report(order_id)
        self.assertEqual(report["order_id"], order_id)
        self.assertEqual(report["symbol"], "GBPUSD")
        self.assertEqual(report["side"], "sell")
        self.assertEqual(report["requested_size"], 5000)
        self.assertEqual(report["requested_price"], 150000)
        self.assertGreaterEqual(report["executed_size"], 0)
        self.assertGreaterEqual(report["executed_price"], 0)
        self.assertGreaterEqual(report["latency_ns"], 0)
        self.assertIn(report["status"], ("filled", "partial", "rejected"))

    def test_get_execution_report_invalid_id(self):
        with self.assertRaises(ValueError):
            self.engine.get_execution_report(999)

    def test_get_execution_report_float_id(self):
        with self.assertRaises(ValueError):
            self.engine.get_execution_report(1.0)

    def test_get_execution_report_non_int_id(self):
        with self.assertRaises(ValueError):
            self.engine.get_execution_report("1")

    # ----- get_order_count -----
    def test_get_order_count_initial(self):
        engine = ExecutionEngine()
        self.assertEqual(engine.get_order_count(), 0)

    def test_get_order_count_after_submit(self):
        engine = ExecutionEngine()
        engine.submit_order("A", "buy", 10, 100)
        engine.submit_order("B", "sell", 20, 200)
        self.assertEqual(engine.get_order_count(), 2)

    def test_get_order_count_type(self):
        self.assertIsInstance(self.engine.get_order_count(), int)

    # ----- determinism -----
    def test_determinism_submit(self):
        engine1 = ExecutionEngine()
        engine2 = ExecutionEngine()
        id1a = engine1.submit_order("EURUSD", "buy", 10000, 100000)
        id1b = engine1.submit_order("EURUSD", "buy", 10000, 100000)
        id2a = engine2.submit_order("EURUSD", "buy", 10000, 100000)
        id2b = engine2.submit_order("EURUSD", "buy", 10000, 100000)
        self.assertEqual(id1a, id2a)
        self.assertEqual(id1b, id2b)
        self.assertEqual(engine1.get_order_count(), engine2.get_order_count())

    def test_determinism_report(self):
        engine1 = ExecutionEngine()
        engine2 = ExecutionEngine()
        id1 = engine1.submit_order("GBPUSD", "sell", 5000, 150000)
        id2 = engine2.submit_order("GBPUSD", "sell", 5000, 150000)
        report1 = engine1.get_execution_report(id1)
        report2 = engine2.get_execution_report(id2)
        self.assertEqual(report1, report2)

    def test_determinism_custom_params(self):
        engine1 = ExecutionEngine(base_latency_ns=2000, slippage_factor=2)
        engine2 = ExecutionEngine(base_latency_ns=2000, slippage_factor=2)
        id1 = engine1.submit_order("EURUSD", "buy", 100, 1000)
        id2 = engine2.submit_order("EURUSD", "buy", 100, 1000)
        self.assertEqual(id1, id2)
        self.assertEqual(engine1.get_execution_report(id1), engine2.get_execution_report(id2))


if __name__ == "__main__":
    unittest.main()