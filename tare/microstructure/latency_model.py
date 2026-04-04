"""
latency_model.py — LatencyModel v1
TARE (Tick-Level Algorithmic Research Environment)

Модель задержки от отправки ордера до исполнения.
Детерминистичная, целочисленная, без случайности.
"""

from tare.time_engine.time_engine import TimeEngine


class LatencyModel:
    """
    Моделирует задержку между временем отправки ордера (order_time_us)
    и временем его исполнения.

    Правила:
      - Только int, никакого float
      - Никакой случайности
      - Детерминизм: одинаковый order_time_us → одинаковый результат
      - Использует TimeEngine для строгой монотонности simulation_time
    """

    def __init__(self, profile: dict) -> None:
        """
        Инициализация LatencyModel.

        Args:
            profile: словарь с параметрами модели. Должен содержать:
                - 'base_latency_us' (int): базовая задержка в микросекундах
                - 'time_engine' (TimeEngine): экземпляр TimeEngine

        Raises:
            ValueError: если параметры некорректны или отсутствуют
        """
        if not isinstance(profile, dict):
            raise ValueError("profile должен быть словарём")

        required_keys = {'base_latency_us', 'time_engine'}
        missing_keys = required_keys - set(profile.keys())
        if missing_keys:
            raise ValueError(f"В profile отсутствуют ключи: {missing_keys}")

        base_latency_us = profile['base_latency_us']
        time_engine = profile['time_engine']

        if not isinstance(base_latency_us, int) or base_latency_us < 0:
            raise ValueError("base_latency_us должен быть неотрицательным int")
        if not isinstance(time_engine, TimeEngine):
            raise ValueError("time_engine должен быть экземпляром TimeEngine")

        self._base_latency_us: int = base_latency_us
        self._time_engine: TimeEngine = time_engine

    def simulate_latency(self, order_time_us: int) -> int:
        """
        Симулирует задержку и возвращает время исполнения ордера.

        Алгоритм:
          1. Преобразуем order_time_us в наносекунды (order_time_ns)
          2. Применяем базовую задержку (base_latency_us → base_latency_ns)
          3. Получаем simulation_time через TimeEngine для гарантии монотонности
          4. Возвращаем execution_time_us = order_time_us + base_latency_us

        Args:
            order_time_us: время отправки ордера в микросекундах (int)

        Returns:
            Время исполнения ордера в микросекундах (int)
        """
        if not isinstance(order_time_us, int):
            raise ValueError("order_time_us должен быть int")

        # 1. Преобразуем в наносекунды для TimeEngine
        order_time_ns = order_time_us * 1000

        # 2. Применяем задержку через TimeEngine
        #    TimeEngine работает в наносекундах, поэтому конвертируем
        base_latency_ns = self._base_latency_us * 1000
        local_time_ns = order_time_ns + base_latency_ns

        # 3. Получаем simulation_time для гарантии монотонности
        #    (хотя в этой модели он не используется напрямую для расчёта времени)
        self._time_engine.process_event(order_time_ns)

        # 4. Возвращаем время исполнения в микросекундах
        execution_time_us = order_time_us + self._base_latency_us
        return execution_time_us

    # ------------------------------------------------------------------
    # Вспомогательные свойства (read-only)
    # ------------------------------------------------------------------

    @property
    def base_latency_us(self) -> int:
        """Базовая задержка в микросекундах."""
        return self._base_latency_us

    @property
    def time_engine(self) -> TimeEngine:
        """Связанный экземпляр TimeEngine."""
        return self._time_engine


# ------------------------------------------------------------------
# Самотест
# ------------------------------------------------------------------

if __name__ == "__main__":
    print("=== LatencyModel v1 — самотест ===\n")

    # Создаём TimeEngine с нулевой задержкой для простоты теста
    time_engine = TimeEngine(base_latency_ns=0)
    profile = {
        'base_latency_us': 50,  # 50 микросекунд
        'time_engine': time_engine
    }

    model = LatencyModel(profile)

    # Тест 1: Базовый расчёт
    order_time = 1_000_000  # 1 секунда в микросекундах
    execution_time = model.simulate_latency(order_time)
    expected = order_time + 50
    assert execution_time == expected, f"Ожидалось {expected}, получено {execution_time}"
    print(f"  ✓ Базовый расчёт: {order_time} + 50 = {execution_time}")

    # Тест 2: Детерминизм
    time_engine.reset()
    execution_time2 = model.simulate_latency(order_time)
    assert execution_time == execution_time2, "Детерминизм нарушен"
    print("  ✓ Детерминизм подтверждён")

    # Тест 3: Проверка типов
    assert isinstance(execution_time, int), "Результат должен быть int"
    print("  ✓ Тип результата int")

    # Тест 4: Несколько вызовов
    time_engine.reset()
    times = [1_000_000, 1_000_100, 1_000_200]
    results = [model.simulate_latency(t) for t in times]
    expected_results = [t + 50 for t in times]
    assert results == expected_results, "Множественные вызовы дали неверный результат"
    print("  ✓ Множественные вызовы работают корректно")

    # Тест 5: Проверка использования TimeEngine
    assert time_engine.events_processed == 3, "TimeEngine должен обработать 3 события"
    print(f"  ✓ TimeEngine обработал {time_engine.events_processed} событий")

    print("\n=== PASSED ===")
