"""
Тесты для модуля latency_model.
"""

import unittest
from tare.time_engine.time_engine import TimeEngine
from tare.microstructure.latency_model import LatencyModel


class TestLatencyModel(unittest.TestCase):
    """Тесты для класса LatencyModel."""

    def setUp(self):
        """Подготовка тестового окружения."""
        self.time_engine = TimeEngine(base_latency_ns=0)
        self.profile = {
            'base_latency_us': 100,
            'time_engine': self.time_engine
        }

    def test_init_valid(self):
        """Тест корректной инициализации."""
        model = LatencyModel(self.profile)
        self.assertEqual(model.base_latency_us, 100)
        self.assertIs(model.time_engine, self.time_engine)

    def test_init_invalid_profile_type(self):
        """Тест инициализации с некорректным типом profile."""
        with self.assertRaises(ValueError):
            LatencyModel("not a dict")

    def test_init_missing_keys(self):
        """Тест инициализации с отсутствующими ключами."""
        with self.assertRaises(ValueError):
            LatencyModel({'base_latency_us': 100})
        with self.assertRaises(ValueError):
            LatencyModel({'time_engine': self.time_engine})

    def test_init_invalid_base_latency(self):
        """Тест инициализации с некорректной базовой задержкой."""
        invalid_profile = self.profile.copy()
        invalid_profile['base_latency_us'] = -1
        with self.assertRaises(ValueError):
            LatencyModel(invalid_profile)

        invalid_profile['base_latency_us'] = 10.5
        with self.assertRaises(ValueError):
            LatencyModel(invalid_profile)

    def test_init_invalid_time_engine(self):
        """Тест инициализации с некорректным time_engine."""
        invalid_profile = self.profile.copy()
        invalid_profile['time_engine'] = "not a TimeEngine"
        with self.assertRaises(ValueError):
            LatencyModel(invalid_profile)

    def test_simulate_latency_basic(self):
        """Тест базового расчёта задержки."""
        model = LatencyModel(self.profile)
        order_time = 1_000_000
        result = model.simulate_latency(order_time)
        expected = 1_000_100
        self.assertEqual(result, expected)
        self.assertIsInstance(result, int)

    def test_simulate_latency_zero_latency(self):
        """Тест с нулевой задержкой."""
        profile = self.profile.copy()
        profile['base_latency_us'] = 0
        model = LatencyModel(profile)
        order_time = 1_000_000
        result = model.simulate_latency(order_time)
        self.assertEqual(result, order_time)

    def test_simulate_latency_invalid_input(self):
        """Тест с некорректным входным значением."""
        model = LatencyModel(self.profile)
        with self.assertRaises(ValueError):
            model.simulate_latency(1000.5)

    def test_simulate_latency_determinism(self):
        """Тест детерминизма."""
        model = LatencyModel(self.profile)
        order_time = 1_000_000
        
        # Первый вызов
        self.time_engine.reset()
        result1 = model.simulate_latency(order_time)
        
        # Второй вызов после reset
        self.time_engine.reset()
        result2 = model.simulate_latency(order_time)
        
        self.assertEqual(result1, result2)

    def test_simulate_latency_multiple_calls(self):
        """Тест множественных вызовов."""
        model = LatencyModel(self.profile)
        order_times = [1_000_000, 1_000_050, 1_000_100]
        
        self.time_engine.reset()
        results = [model.simulate_latency(t) for t in order_times]
        
        expected = [t + 100 for t in order_times]
        self.assertEqual(results, expected)
        
        # Проверяем что TimeEngine обработал все события
        self.assertEqual(self.time_engine.events_processed, 3)

    def test_simulate_latency_time_engine_interaction(self):
        """Тест взаимодействия с TimeEngine."""
        # Используем TimeEngine с ненулевой задержкой
        time_engine = TimeEngine(base_latency_ns=5000)  # 5 микросекунд
        profile = {
            'base_latency_us': 100,
            'time_engine': time_engine
        }
        model = LatencyModel(profile)
        
        order_time_us = 1_000_000
        result = model.simulate_latency(order_time_us)
        
        # Проверяем что наша модель добавляет свою задержку
        self.assertEqual(result, order_time_us + 100)
        
        # Проверяем что TimeEngine обработал событие
        self.assertEqual(time_engine.events_processed, 1)

    def test_properties(self):
        """Тест свойств класса."""
        model = LatencyModel(self.profile)
        self.assertEqual(model.base_latency_us, 100)
        self.assertIs(model.time_engine, self.time_engine)


if __name__ == '__main__':
    unittest.main()
