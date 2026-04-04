"""
test_time_engine.py — Юнит-тесты для TimeEngine v1
TARE (Tick-Level Algorithmic Research Environment)

Запуск:
    python -m pytest tare/time_engine/test_time_engine.py -v
    python tare/time_engine/test_time_engine.py          # без pytest
"""

import sys
import os
import unittest

# Позволяет запускать напрямую без установки пакета
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tare.time_engine.time_engine import TimeEngine


class TestTimeEngineInit(unittest.TestCase):
    """Тесты инициализации."""

    def test_default_latency(self):
        """По умолчанию latency = 1000 нс."""
        engine = TimeEngine()
        self.assertEqual(engine.base_latency_ns, 1_000)

    def test_custom_latency(self):
        """Кастомная latency устанавливается корректно."""
        engine = TimeEngine(base_latency_ns=500_000)
        self.assertEqual(engine.base_latency_ns, 500_000)

    def test_zero_latency_allowed(self):
        """Нулевая задержка допустима."""
        engine = TimeEngine(base_latency_ns=0)
        self.assertEqual(engine.base_latency_ns, 0)

    def test_invalid_latency_negative(self):
        """Отрицательная latency — ValueError."""
        with self.assertRaises(ValueError):
            TimeEngine(base_latency_ns=-1)

    def test_invalid_latency_float(self):
        """Float latency — ValueError."""
        with self.assertRaises(ValueError):
            TimeEngine(base_latency_ns=1000.5)

    def test_initial_counters_zero(self):
        """После инициализации все счётчики = 0."""
        engine = TimeEngine()
        self.assertEqual(engine.events_processed, 0)
        self.assertEqual(engine.current_sim_time, 0)


class TestApplyLatency(unittest.TestCase):
    """Тесты метода apply_latency."""

    def setUp(self):
        self.engine = TimeEngine(base_latency_ns=1_000)

    def test_basic(self):
        """local_time = market_time + latency."""
        result = self.engine.apply_latency(1_000_000_000)
        self.assertEqual(result, 1_000_001_000)

    def test_zero_market_time(self):
        """market_time=0 работает корректно."""
        result = self.engine.apply_latency(0)
        self.assertEqual(result, 1_000)

    def test_zero_latency(self):
        """При нулевой задержке local_time == market_time."""
        engine = TimeEngine(base_latency_ns=0)
        self.assertEqual(engine.apply_latency(999), 999)

    def test_result_is_int(self):
        """Результат всегда int."""
        result = self.engine.apply_latency(123_456_789)
        self.assertIsInstance(result, int)

    def test_large_timestamp(self):
        """Большие наносекундные timestamps (реальные биржевые данные)."""
        # 2024-01-01 00:00:00 UTC в наносекундах ≈ 1_704_067_200_000_000_000
        market_time = 1_704_067_200_000_000_000
        result = self.engine.apply_latency(market_time)
        self.assertEqual(result, market_time + 1_000)
        self.assertIsInstance(result, int)


class TestNextSimTime(unittest.TestCase):
    """Тесты метода next_sim_time."""

    def setUp(self):
        self.engine = TimeEngine()

    def test_starts_at_one(self):
        """Первый вызов возвращает 1."""
        self.assertEqual(self.engine.next_sim_time(), 1)

    def test_strictly_monotonic(self):
        """Каждый вызов больше предыдущего."""
        values = [self.engine.next_sim_time() for _ in range(100)]
        for i in range(1, len(values)):
            self.assertGreater(values[i], values[i - 1])

    def test_unique_values(self):
        """Все значения уникальны."""
        values = [self.engine.next_sim_time() for _ in range(100)]
        self.assertEqual(len(set(values)), 100)

    def test_all_int(self):
        """Все значения int."""
        for _ in range(10):
            v = self.engine.next_sim_time()
            self.assertIsInstance(v, int)

    def test_sequential(self):
        """Значения идут строго 1, 2, 3, ..."""
        engine = TimeEngine()
        for expected in range(1, 11):
            self.assertEqual(engine.next_sim_time(), expected)


class TestProcessEvent(unittest.TestCase):
    """Тесты метода process_event."""

    def setUp(self):
        self.engine = TimeEngine(base_latency_ns=500)

    def test_returns_dict(self):
        """Возвращает словарь."""
        result = self.engine.process_event(1_000_000)
        self.assertIsInstance(result, dict)

    def test_required_keys(self):
        """Словарь содержит все обязательные ключи."""
        result = self.engine.process_event(1_000_000)
        required = {"market_time", "local_time", "simulation_time",
                    "sequence", "latency_ns"}
        self.assertEqual(set(result.keys()), required)

    def test_market_time_preserved(self):
        """market_time не изменяется."""
        ts = 1_234_567_890
        result = self.engine.process_event(ts)
        self.assertEqual(result["market_time"], ts)

    def test_local_time_correct(self):
        """local_time = market_time + latency."""
        result = self.engine.process_event(1_000_000)
        self.assertEqual(result["local_time"], 1_000_500)

    def test_latency_in_result(self):
        """latency_ns в результате совпадает с base_latency_ns."""
        result = self.engine.process_event(1_000_000)
        self.assertEqual(result["latency_ns"], 500)

    def test_all_values_int(self):
        """Все значения в словаре — int. Никакого float."""
        result = self.engine.process_event(1_000_000)
        for key, value in result.items():
            self.assertIsInstance(value, int,
                                  msg=f"Поле '{key}' = {value!r} не является int")

    def test_simulation_time_monotonic(self):
        """simulation_time строго возрастает."""
        events = [self.engine.process_event(t) for t in
                  [100, 200, 200, 300, 100]]  # включая неупорядоченные и дубли
        sim_times = [e["simulation_time"] for e in events]
        for i in range(1, len(sim_times)):
            self.assertGreater(sim_times[i], sim_times[i - 1],
                               msg="simulation_time не монотонен")

    def test_sequence_increments(self):
        """sequence строго увеличивается на 1."""
        events = [self.engine.process_event(t) for t in range(5)]
        sequences = [e["sequence"] for e in events]
        self.assertEqual(sequences, [1, 2, 3, 4, 5])

    def test_identical_market_times(self):
        """Одинаковые market_time дают разные simulation_time (tie-breaking)."""
        e1 = self.engine.process_event(1_000_000)
        e2 = self.engine.process_event(1_000_000)
        self.assertNotEqual(e1["simulation_time"], e2["simulation_time"])
        self.assertEqual(e1["market_time"], e2["market_time"])

    def test_events_processed_counter(self):
        """events_processed считает корректно."""
        for i in range(7):
            self.engine.process_event(i * 100)
        self.assertEqual(self.engine.events_processed, 7)


class TestReset(unittest.TestCase):
    """Тесты метода reset."""

    def setUp(self):
        self.engine = TimeEngine(base_latency_ns=1_000)

    def test_reset_clears_counters(self):
        """После reset() счётчики обнуляются."""
        for t in [100, 200, 300]:
            self.engine.process_event(t)

        self.engine.reset()

        self.assertEqual(self.engine.events_processed, 0)
        self.assertEqual(self.engine.current_sim_time, 0)

    def test_determinism_after_reset(self):
        """Одинаковая последовательность → одинаковый результат после reset."""
        timestamps = [1_000, 2_000, 2_000, 3_000]

        run1 = [self.engine.process_event(t) for t in timestamps]
        self.engine.reset()
        run2 = [self.engine.process_event(t) for t in timestamps]

        self.assertEqual(run1, run2,
                         "Детерминизм нарушен: два прогона дали разные результаты")

    def test_reset_does_not_change_latency(self):
        """reset() не меняет base_latency_ns."""
        self.engine.reset()
        self.assertEqual(self.engine.base_latency_ns, 1_000)

    def test_sim_time_restarts_from_one(self):
        """После reset() simulation_time снова начинается с 1."""
        self.engine.process_event(100)
        self.engine.reset()
        result = self.engine.process_event(100)
        self.assertEqual(result["simulation_time"], 1)

    def test_multiple_resets(self):
        """Несколько reset() подряд безопасны."""
        self.engine.reset()
        self.engine.reset()
        self.engine.reset()
        result = self.engine.process_event(100)
        self.assertEqual(result["sequence"], 1)


class TestDeterminism(unittest.TestCase):
    """Интеграционные тесты на детерминизм — главное свойство TARE."""

    def test_million_ticks_deterministic(self):
        """1 млн тиков → одинаковый результат при двух прогонах."""
        engine1 = TimeEngine(base_latency_ns=250)
        engine2 = TimeEngine(base_latency_ns=250)

        # Детерминированная последовательность (без random)
        timestamps = [i * 100 + (i % 7) * 13 for i in range(1_000_000)]

        last1 = None
        last2 = None
        for ts in timestamps:
            last1 = engine1.process_event(ts)
            last2 = engine2.process_event(ts)

        self.assertEqual(last1, last2)
        self.assertEqual(engine1.events_processed, engine2.events_processed)

    def test_no_float_in_million_ticks(self):
        """Ни одного float в 1 млн событий."""
        engine = TimeEngine(base_latency_ns=100)
        for i in range(1_000_000):
            result = engine.process_event(i * 1_000)
            for v in result.values():
                if not isinstance(v, int):
                    self.fail(f"Обнаружен float на событии {i}: {v!r}")


# ------------------------------------------------------------------
# Запуск без pytest
# ------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main(verbosity=2)
