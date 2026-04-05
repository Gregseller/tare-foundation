import unittest
from tare.live.websocket_connector import WebSocketConnector


class TestWebSocketConnector(unittest.TestCase):
    def setUp(self):
        self.config = {
            "host": "ws.test.com",
            "port": 9000,
            "api_key": "test_key",
            "api_secret": "test_secret"
        }
        self.connector = WebSocketConnector(self.config)

    # ----- __init__ -----
    def test_init_valid(self):
        conn = WebSocketConnector(self.config)
        self.assertIsInstance(conn, WebSocketConnector)

    def test_init_with_max_buffer(self):
        cfg = self.config.copy()
        cfg["max_buffer_size"] = 5000
        conn = WebSocketConnector(cfg)
        self.assertIsInstance(conn, WebSocketConnector)

    def test_init_missing_host(self):
        cfg = self.config.copy()
        del cfg["host"]
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

    def test_init_host_not_str(self):
        cfg = self.config.copy()
        cfg["host"] = 123
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

    def test_init_missing_port(self):
        cfg = self.config.copy()
        del cfg["port"]
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

    def test_init_port_not_int(self):
        cfg = self.config.copy()
        cfg["port"] = "8080"
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

    def test_init_port_non_positive(self):
        cfg = self.config.copy()
        cfg["port"] = 0
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

    def test_init_missing_api_key(self):
        cfg = self.config.copy()
        del cfg["api_key"]
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

    def test_init_api_key_not_str(self):
        cfg = self.config.copy()
        cfg["api_key"] = 123
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

    def test_init_missing_api_secret(self):
        cfg = self.config.copy()
        del cfg["api_secret"]
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

    def test_init_api_secret_not_str(self):
        cfg = self.config.copy()
        cfg["api_secret"] = 123
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

    def test_init_max_buffer_not_int(self):
        cfg = self.config.copy()
        cfg["max_buffer_size"] = "10000"
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

    def test_init_max_buffer_non_positive(self):
        cfg = self.config.copy()
        cfg["max_buffer_size"] = 0
        with self.assertRaises(ValueError):
            WebSocketConnector(cfg)

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

    def test_subscribe_not_connected(self):
        with self.assertRaises(RuntimeError):
            self.connector.subscribe(["EURUSD"])

    def test_subscribe_not_list(self):
        self.connector.connect()
        with self.assertRaises(ValueError):
            self.connector.subscribe("EURUSD")

    def test_subscribe_empty_list(self):
        self.connector.connect()
        self.connector.subscribe([])

    def test_subscribe_invalid_symbol(self):
        self.connector.connect()
        with self.assertRaises(ValueError):
            self.connector.subscribe([123])

    # ----- unsubscribe -----
    def test_unsubscribe_valid(self):
        self.connector.connect()
        self.connector.subscribe(["EURUSD"])
        self.connector.unsubscribe(["EURUSD"])

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
        oid = self.connector.send_order(order)
        self.assertIsInstance(oid, str)
        self.assertGreater(len(oid), 0)

    def test_send_order_valid_sell(self):
        self.connector.connect()
        order = {"symbol": "GBPUSD", "side": "sell", "size": 5000, "price": 150000}
        oid = self.connector.send_order(order)
        self.assertIsInstance(oid, str)

    def test_send_order_not_connected(self):
        with self.assertRaises(RuntimeError):
            self.connector.send_order({"symbol": "EURUSD", "side": "buy", "size": 100, "price": 100})

    def test_send_order_missing_key(self):
        self.connector.connect()
        bad = {"symbol": "EURUSD", "side": "buy", "size": 100}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad)

    def test_send_order_invalid_side(self):
        self.connector.connect()
        bad = {"symbol": "EURUSD", "side": "invalid", "size": 100, "price": 100}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad)

    def test_send_order_size_not_int(self):
        self.connector.connect()
        bad = {"symbol": "EURUSD", "side": "buy", "size": 100.5, "price": 100}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad)

    def test_send_order_size_non_positive(self):
        self.connector.connect()
        bad = {"symbol": "EURUSD", "side": "buy", "size": 0, "price": 100}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad)

    def test_send_order_price_not_int(self):
        self.connector.connect()
        bad = {"symbol": "EURUSD", "side": "buy", "size": 100, "price": 100.5}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad)

    def test_send_order_price_non_positive(self):
        self.connector.connect()
        bad = {"symbol": "EURUSD", "side": "buy", "size": 100, "price": 0}
        with self.assertRaises(ValueError):
            self.connector.send_order(bad)

    def test_send_order_deterministic_ids(self):
        self.connector.connect()
        order = {"symbol": "EURUSD", "side": "buy", "size": 100, "price": 100}
        id1 = self.connector.send_order(order)
        id2 = self.connector.send_order(order)
        self.assertNotEqual(id1, id2)
        conn2 = WebSocketConnector(self.config)
        conn2.connect()
        id1b = conn2.send_order(order)
        id2b = conn2.send_order(order)
        self.assertEqual(id1, id1b)
        self.assertEqual(id2, id2b)

    # ----- cancel_order -----
    def test_cancel_order_success(self):
        self.connector.connect()
        order = {"symbol": "EURUSD", "side": "buy", "size": 100, "price": 100}
        oid = self.connector.send_order(order)
        result = self.connector.cancel_order(oid)
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
        conn2 = WebSocketConnector(self.config)
        conn2.connect()
        conn2.inject_tick(tick)
        msg2 = conn2.recv_message()
        self.assertEqual(msg1, msg2)

    def test_inject_tick_invalid(self):
        self.connector.connect()
        with self.assertRaises(ValueError):
            self.connector.inject_tick("not dict")
        with self.assertRaises(ValueError):
            self.connector.inject_tick({"symbol": "EURUSD"})  # missing fields

    def test_recv_message_after_order(self):
        self.connector.connect()
        order = {"symbol": "EURUSD", "side": "buy", "size": 1000, "price": 120000}
        oid = self.connector.send_order(order)
        msg = self.connector.recv_message()
        if msg:
            self.assertIn("type", msg)
            self.assertIn("order_id", msg)

    # ----- determinism of whole sequence -----
    def test_deterministic_sequence(self):
        def run():
            conn = WebSocketConnector(self.config)
            conn.connect()
            conn.subscribe(["EURUSD"])
            oid = conn.send_order({"symbol": "EURUSD", "side": "buy", "size": 100, "price": 100})
            conn.cancel_order(oid)
            conn.inject_tick({"symbol": "EURUSD", "bid": 100, "ask": 101, "timestamp": 1})
            msg = conn.recv_message()
            conn.disconnect()
            return (oid, msg)

        r1 = run()
        r2 = run()
        self.assertEqual(r1, r2)


if __name__ == "__main__":
    unittest.main()
