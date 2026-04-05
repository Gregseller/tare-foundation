import unittest
from tare.time_engine.time_engine import TimeEngine
from tare.tick_data_engine.tick_reader import TickReader
from tare.execution.execution_engine import ExecutionEngine
from tare.execution.portfolio_engine import PortfolioEngine
from tare.validation.adequacy_v1 import AdequacyV1


class TestIntegration(unittest.TestCase):
    def test_time_engine_to_execution(self):
        time_engine = TimeEngine(base_latency_ns=1000)
        exec_engine = ExecutionEngine(base_latency_ns=1000)
        order_ids = []
        for _ in range(3):
            oid = exec_engine.submit_order("EURUSD", "buy", 1000, 100000)
            order_ids.append(oid)
        self.assertEqual(len(set(order_ids)), 3)
        for oid in order_ids:
            self.assertIsInstance(oid, int)
        self.assertEqual(exec_engine.get_order_count(), 3)

    def test_execution_to_portfolio(self):
        exec_engine = ExecutionEngine()
        portfolio = PortfolioEngine()
        order_id = exec_engine.submit_order("GBPUSD", "buy", 5000, 150000)
        report = exec_engine.get_execution_report(order_id)
        portfolio.update_position(
            symbol=report["symbol"],
            side=report["side"],
            size=report["executed_size"],
            price=report["executed_price"]
        )
        self.assertGreater(portfolio.get_position("GBPUSD"), 0)
        pnl = portfolio.get_pnl("GBPUSD", 151000)
        self.assertIsInstance(pnl, int)

    def test_adequacy_validates_ticks(self):
        validator = AdequacyV1(min_ticks=3)
        timestamps = [1000, 2000, 3000, 4000, 5000]
        spreads = [10, 20, 30, 40, 50]
        summary = validator.get_summary(
            tick_count=5,
            timestamps=timestamps,
            spreads=spreads,
            max_spread=100
        )
        self.assertTrue(summary["adequate"])

    def test_full_pipeline_determinism(self):
        def run_pipeline():
            exec_engine = ExecutionEngine()
            portfolio = PortfolioEngine()
            oid = exec_engine.submit_order("EURUSD", "buy", 1000, 100000)
            report = exec_engine.get_execution_report(oid)
            portfolio.update_position(
                symbol=report["symbol"],
                side=report["side"],
                size=report["executed_size"],
                price=report["executed_price"]
            )
            return portfolio.get_position("EURUSD"), portfolio.get_pnl("EURUSD", 101000)

        result1 = run_pipeline()
        result2 = run_pipeline()
        self.assertEqual(result1, result2)


if __name__ == "__main__":
    unittest.main()
