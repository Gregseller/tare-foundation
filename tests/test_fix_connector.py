import unittest
from tare.live.fix_connector import FIXConnector


class TestFIXConnector(unittest.TestCase):
    def setUp(self):
        self.config = {
            "sender_comp_id": "SENDER123",
            "target_comp_id": "TARGET456"
        }
        self.connector = FIXConnector(self.config)

    # ----- __init__ -----
    def test_init_valid(self):
        connector = FIXConnector(self.config)
        self.assertIsInstance(connector, FIXConnector)

    def test_init_missing_key(self):
        bad_config = {"host": "localhost"}
        with self.assertRaises(ValueError):
            FIXConnector(bad_config)

    def test_init_extra_key_ok(self):
        extra_config = self.config.copy()
        extra_config["extra"] = "value"
        connector = FIXConnector(extra_config)
        self.assertIsInstance(connector, FIXConnector)

    def test_init_not_dict(self):
        with self.assertRaises(ValueError):
            FIXConnector("not dict")

    # ----- connect / disconnect / is_connected -----
    def test_initial_not_connected(self):
        self.assertFalse(self.connector.is_connected())

    def test_connect_sets_connected(self):
        self.connector.connect()
        self.assertTrue(self.connector.is_connected())

    def test_disconnect_sets_not_connected(self):
        self.connector.connect()
        self.connector.disconnect()
        self.assertFalse(self.connector.is_connected())

    def test_connect_idempotent(self):
        self.connector.connect()
        self.connector.connect()
        self.assertTrue(self.connector.is_connected())

    def test_disconnect_idempotent(self):
        self.connector.disconnect()
        self.assertFalse(self.connector.is_connected())

    # ----- subscribe -----
    def test_subscribe_valid(self):
        self.connector.connect()
        symbols = ["EURUSD", "GBPUSD"]
        self.connector.subscribe(symbols)
        # Не вызывает ошибок, можно проверить внутреннее состояние через inject_tick/recv_message

    def test_subscribe_not_connected(self):
        with self.assertRaises(RuntimeError):
            self.connector.subscribe(["EURUSD"])

    def test_subscribe_not_list(self):
        self.connector.connect()
        with self.assertRaises(ValueError):
            self.connector.subscribe("EURUSD")

    def test_subscribe_empty_list(self):
        self.connector.connect()
        self.connector.subscribe([])  # должно быть допустимо

    def test_subscribe_invalid_symbol(self):
        self.connector.connect()
        with self.assertRaises(ValueError):
            self.connector.subscribe([123])

    # ----- unsubscribe -----
    def test_unsubscribe_valid(self):
        self.connector.connect()
        self.connector.subscribe(["EURUSD"])
        self.connector.unsubscribe(["EURUSD"])
        # не падает

    def test_unsubscribe_not_connected(self):
        with self.assertRaises(RuntimeError):
            self.connector.unsubscribe(["EURUSD"])

    def test_unsubscribe_not_list(self):
        self.connector.connect()
        with self.assertRaises(ValueError):
            self.connector.unsubscribe("EURUSD")

    # ----- send_order -----
    def test_send_order_valid_buy(self):
        self.connector.connect()
        order = {"symbol": "EURUSD", "side": "buy", "size": 10000, "price": 120000}
        order_id = self.connector.send_order(order)
        self.assertIsInstance(order_id, str)
        self.assertGreater(len(order_id), 0)

    def test_send_order_valid_sell(self):
        self.connector.connect()
        order = {"symbol": "GBPUSD", "side": "sell", "size": 5000, "price": 150000}
        order_id = self.connector.send_order(order)
        self.assertIsInstance(order_id, str)

    def test_send_order_not_connected(self):
        with self.assertRaises(RuntimeError):
            self.connector.send_order({"symbol": "EURUSD", "side": "buy", "size": 100, "price": 100})

    def test_send_order_missing_key(self):
        self.connector.connect()
        bad_order = {"symbol": "EURUSD", "side": "buy", "size": 100}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad_order)

    def test_send_order_invalid_side(self):
        self.connector.connect()
        bad_order = {"symbol": "EURUSD", "side": "invalid", "size": 100, "price": 100}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad_order)

    def test_send_order_size_not_int(self):
        self.connector.connect()
        bad_order = {"symbol": "EURUSD", "side": "buy", "size": 100.5, "price": 100}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad_order)

    def test_send_order_size_non_positive(self):
        self.connector.connect()
        bad_order = {"symbol": "EURUSD", "side": "buy", "size": 0, "price": 100}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad_order)

    def test_send_order_price_not_int(self):
        self.connector.connect()
        bad_order = {"symbol": "EURUSD", "side": "buy", "size": 100, "price": 100.5}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad_order)

    def test_send_order_price_non_positive(self):
        self.connector.connect()
        bad_order = {"symbol": "EURUSD", "side": "buy", "size": 100, "price": 0}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad_order)

    def test_send_order_deterministic_id(self):
        self.connector.connect()
        order = {"symbol": "EURUSD", "side": "buy", "size": 100, "price": 100}
        id1 = self.connector.send_order(order)
        id2 = self.connector.send_order(order)
        self.assertNotEqual(id1, id2)  # каждый ордер уникальный
        # но при повторном создании нового коннектора порядок должен совпадать
        connector2 = FIXConnector(self.config)
        connector2.connect()
        id1b = connector2.send_order(order)
        id2b = connector2.send_order(order)
        self.assertEqual(id1, id1b)
        self.assertEqual(id2, id2b)

    # ----- cancel_order -----
    def test_cancel_order_success(self):
        self.connector.connect()
        order = {"symbol": "EURUSD", "side": "buy", "size": 100, "price": 100}
        order_id = self.connector.send_order(order)
        result = self.connector.cancel_order(order_id)
        self.assertTrue(result)

    def test_cancel_order_not_connected(self):
        with self.assertRaises(RuntimeError):
            self.connector.cancel_order("123")

    def test_cancel_order_invalid_id(self):
        self.connector.connect()
        result = self.connector.cancel_order("nonexistent")
        self.assertFalse(result)

    def test_cancel_order_id_not_str(self):
        self.connector.connect()
        with self.assertRaises(ValueError):
            self.connector.cancel_order(123)

    # ----- inject_tick and recv_message -----
    def test_inject_tick_and_recv(self):
        self.connector.connect()
        tick = {"symbol": "EURUSD", "bid": 120000, "ask": 120010, "timestamp": 1000}
        self.connector.inject_tick(tick)
        msg = self.connector.recv_message()
        self.assertIsInstance(msg, dict)
        self.assertEqual(msg.get("type"), "tick")
        self.assertEqual(msg.get("symbol"), "EURUSD")

    def test_recv_message_no_message(self):
        self.connector.connect()
        msg = self.connector.recv_message()
        self.assertIsNone(msg)

    def test_inject_tick_determinism(self):
        self.connector.connect()
        tick = {"symbol": "EURUSD", "bid": 120000, "ask": 120010, "timestamp": 1000}
        self.connector.inject_tick(tick)
        msg1 = self.connector.recv_message()
        # новый коннектор
        connector2 = FIXConnector(self.config)
        connector2.connect()
        connector2.inject_tick(tick)
        msg2 = connector2.recv_message()
        self.assertEqual(msg1, msg2)

    def test_inject_tick_invalid(self):
        self.connector.connect()
        with self.assertRaises(ValueError):
            self.connector.inject_tick("not dict")
        with self.assertRaises(ValueError):
            self.connector.inject_tick({"symbol": "EURUSD"})  # missing fields

    # ----- recv_message after order execution -----
    def test_recv_message_after_order(self):
        self.connector.connect()
        order = {"symbol": "EURUSD", "side": "buy", "size": 1000, "price": 120000}
        order_id = self.connector.send_order(order)
        # Возможно, коннектор генерирует сообщение об исполнении
        msg = self.connector.recv_message()
        if msg:
            self.assertIn("type", msg)
            self.assertIn("order_id", msg)

    # ----- determinism of multiple operations -----
    def test_deterministic_sequence(self):
        def run_sequence():
            conn = FIXConnector(self.config)
            conn.connect()
            conn.subscribe(["EURUSD"])
            order_id = conn.send_order({"symbol": "EURUSD", "side": "buy", "size": 100, "price": 100})
            conn.cancel_order(order_id)
            conn.inject_tick({"symbol": "EURUSD", "bid": 100, "ask": 101, "timestamp": 1})
            msg = conn.recv_message()
            conn.disconnect()
            return (order_id, msg)

        r1 = run_sequence()
        r2 = run_sequence()
        self.assertEqual(r1, r2)


if __name__ == "__main__":
    unittest.main()
