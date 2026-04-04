"""
test_smoke.py — Дымовые тесты: импорт и базовая связность.

Если это падает — что-то сломано фундаментально.
"""
import unittest


class TestImports(unittest.TestCase):

    def test_import_tare(self):
        import tare
        self.assertEqual(tare.__version__, "0.1.0")

    def test_import_time_engine(self):
        from tare.time_engine import TimeEngine
        self.assertIsNotNone(TimeEngine)

    def test_time_engine_instantiation(self):
        from tare.time_engine import TimeEngine
        engine = TimeEngine(base_latency_ns=100)
        self.assertEqual(engine.base_latency_ns, 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
