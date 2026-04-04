import unittest
from tare.execution.portfolio_engine import PortfolioEngine


class TestPortfolioEngine(unittest.TestCase):
    def setUp(self):
        self.portfolio = PortfolioEngine()

    def test_init(self):
        self.assertEqual(self.portfolio.get_transaction_count(), 0)
        self.assertEqual(self.portfolio.get_position("EURUSD"), 0)

    def test_update_buy_single(self):
        self.portfolio.update_position("EURUSD", "buy", 1000, 120000)
        self.assertEqual(self.portfolio.get_position("EURUSD"), 1000)
        self.assertEqual(self.portfolio.get_transaction_count(), 1)

    def test_update_buy_multiple_same_symbol(self):
        self.portfolio.update_position("EURUSD", "buy", 1000, 120000)
        self.portfolio.update_position("EURUSD", "buy", 500, 121000)
        self.assertEqual(self.portfolio.get_position("EURUSD"), 1500)
        avg = (1000 * 120000 + 500 * 121000) // 1500
        pnl = self.portfolio.get_pnl("EURUSD", 122000)
        self.assertEqual(pnl, 1500 * (122000 - avg))

    def test_update_sell_reduces_position(self):
        self.portfolio.update_position("EURUSD", "buy", 1000, 120000)
        self.portfolio.update_position("EURUSD", "sell", 400, 121000)
        self.assertEqual(self.portfolio.get_position("EURUSD"), 600)

    def test_update_sell_exact_position(self):
        self.portfolio.update_position("EURUSD", "buy", 1000, 120000)
        self.portfolio.update_position("EURUSD", "sell", 1000, 121000)
        self.assertEqual(self.portfolio.get_position("EURUSD"), 0)
        self.assertEqual(self.portfolio.get_pnl("EURUSD", 122000), 0)

    def test_update_sell_more_than_position_raises(self):
        self.portfolio.update_position("EURUSD", "buy", 1000, 120000)
        with self.assertRaises(ValueError):
            self.portfolio.update_position("EURUSD", "sell", 1500, 121000)

    def test_update_sell_no_position_raises(self):
        with self.assertRaises(ValueError):
            self.portfolio.update_position("EURUSD", "sell", 100, 120000)

    def test_invalid_symbol(self):
        with self.assertRaises(ValueError):
            self.portfolio.update_position(123, "buy", 100, 1000)

    def test_invalid_side(self):
        with self.assertRaises(ValueError):
            self.portfolio.update_position("EURUSD", "BUY", 100, 1000)

    def test_invalid_size(self):
        with self.assertRaises(ValueError):
            self.portfolio.update_position("EURUSD", "buy", 0, 1000)

    def test_invalid_price(self):
        with self.assertRaises(ValueError):
            self.portfolio.update_position("EURUSD", "buy", 100, 0)

    def test_get_position_nonexistent(self):
        self.assertEqual(self.portfolio.get_position("NONEXISTENT"), 0)

    def test_get_pnl_profit(self):
        self.portfolio.update_position("EURUSD", "buy", 1000, 120000)
        self.assertEqual(self.portfolio.get_pnl("EURUSD", 122000), 2000000)

    def test_get_pnl_loss(self):
        self.portfolio.update_position("EURUSD", "buy", 1000, 120000)
        self.assertEqual(self.portfolio.get_pnl("EURUSD", 118000), -2000000)

    def test_get_pnl_invalid_price(self):
        with self.assertRaises(ValueError):
            self.portfolio.get_pnl("EURUSD", 0)

    def test_get_pnl_zero_position(self):
        self.assertEqual(self.portfolio.get_pnl("EURUSD", 120000), 0)

    def test_get_transaction_count(self):
        self.portfolio.update_position("A", "buy", 10, 100)
        self.portfolio.update_position("A", "sell", 5, 101)
        self.assertEqual(self.portfolio.get_transaction_count(), 2)

    def test_determinism(self):
        p1, p2 = PortfolioEngine(), PortfolioEngine()
        p1.update_position("EURUSD", "buy", 1000, 120000)
        p2.update_position("EURUSD", "buy", 1000, 120000)
        self.assertEqual(p1.get_pnl("EURUSD", 122000), p2.get_pnl("EURUSD", 122000))

    def test_all_numeric_fields_are_int(self):
        self.portfolio.update_position("EURUSD", "buy", 1000, 120000)
        self.assertIsInstance(self.portfolio.get_position("EURUSD"), int)
        self.assertIsInstance(self.portfolio.get_pnl("EURUSD", 122000), int)
        self.assertIsInstance(self.portfolio.get_transaction_count(), int)

if __name__ == "__main__":
    unittest.main()
