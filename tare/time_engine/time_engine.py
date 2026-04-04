"""
time_engine.py — TimeEngine v1
TARE (Tick-Level Algorithmic Research Environment)

Единственный источник истины о времени в симуляции.
Три домена: market_time → local_time → simulation_time.

Правила:
  - Только int, никакого float
  - Никакой случайности
  - Строгая монотонность simulation_time
  - Tie-breaking по sequence_counter
"""


class TimeEngine:
    """
    Управляет тремя временными доменами симуляции:

      market_time     — оригинальный timestamp с биржи (int, наносекунды)
      local_time      — время прибытия после смоделированной задержки
      simulation_time — строго монотонный внутренний счётчик

    Все операции целочисленные. Никакого float. Никакой случайности.
    """

    def __init__(self, base_latency_ns: int = 1_000):
        """
        Инициализация TimeEngine.

        Args:
            base_latency_ns: базовая детерминированная задержка в наносекундах.
                             По умолчанию 1000 нс (1 микросекунда).
        """
        if not isinstance(base_latency_ns, int) or base_latency_ns < 0:
            raise ValueError("base_latency_ns должен быть неотрицательным int")

        self._base_latency_ns: int = base_latency_ns
        self._sim_counter: int = 0        # монотонный счётчик simulation_time
        self._seq_counter: int = 0        # tie-breaker для одинаковых timestamps
        self._last_local_time: int = 0    # для контроля монотонности

    # ------------------------------------------------------------------
    # Публичные методы
    # ------------------------------------------------------------------

    def apply_latency(self, market_time: int) -> int:
        """
        Вычисляет local_time: время прибытия тика после задержки.

        Args:
            market_time: timestamp с биржи (int, наносекунды)

        Returns:
            local_time = market_time + base_latency_ns (int)
        """
        return market_time + self._base_latency_ns

    def next_sim_time(self) -> int:
        """
        Возвращает следующий строго монотонный simulation_time.

        Каждый вызов увеличивает внутренний счётчик на 1.
        Гарантирует уникальность и порядок независимо от входных данных.

        Returns:
            simulation_time (int) — уникальный монотонный номер события
        """
        self._sim_counter += 1
        return self._sim_counter

    def process_event(self, market_time: int) -> dict:
        """
        Полный цикл обработки одного тика.

        Преобразует market_time в три временных домена и возвращает
        детерминированный слепок события.

        Args:
            market_time: timestamp с биржи (int, наносекунды)

        Returns:
            dict с ключами:
              market_time     — входной timestamp (int)
              local_time      — market_time + latency (int)
              simulation_time — монотонный счётчик (int)
              sequence        — порядковый номер события (int, с 1)
              latency_ns      — применённая задержка (int)
        """
        self._seq_counter += 1

        local_time = self.apply_latency(market_time)
        sim_time   = self.next_sim_time()

        return {
            "market_time":     market_time,
            "local_time":      local_time,
            "simulation_time": sim_time,
            "sequence":        self._seq_counter,
            "latency_ns":      self._base_latency_ns,
        }

    def reset(self) -> None:
        """
        Сброс всех счётчиков в начальное состояние.

        Используется для повторного детерминированного воспроизведения
        одной и той же последовательности событий.
        """
        self._sim_counter    = 0
        self._seq_counter    = 0
        self._last_local_time = 0

    # ------------------------------------------------------------------
    # Вспомогательные свойства (read-only)
    # ------------------------------------------------------------------

    @property
    def base_latency_ns(self) -> int:
        """Базовая задержка в наносекундах."""
        return self._base_latency_ns

    @property
    def events_processed(self) -> int:
        """Количество обработанных событий с момента последнего reset()."""
        return self._seq_counter

    @property
    def current_sim_time(self) -> int:
        """Текущее значение simulation_time (последнее выданное)."""
        return self._sim_counter


# ------------------------------------------------------------------
# Самотест
# ------------------------------------------------------------------

if __name__ == "__main__":
    print("=== TimeEngine v1 — самотест ===\n")

    engine = TimeEngine(base_latency_ns=500)

    # Базовый прогон
    timestamps = [1_000_000_000, 1_000_000_100, 1_000_000_100, 1_000_000_200]
    results = []

    for ts in timestamps:
        event = engine.process_event(ts)
        results.append(event)
        print(f"  market={event['market_time']:>15}  "
              f"local={event['local_time']:>15}  "
              f"sim={event['simulation_time']:>4}  "
              f"seq={event['sequence']:>4}")

    # Проверка монотонности
    sim_times = [r["simulation_time"] for r in results]
    assert sim_times == sorted(sim_times), "FAIL: simulation_time не монотонен"
    assert len(set(sim_times)) == len(sim_times), "FAIL: simulation_time не уникален"
    print("\n  ✓ simulation_time строго монотонен")

    # Проверка детерминизма: reset + повтор = тот же результат
    engine.reset()
    results2 = [engine.process_event(ts) for ts in timestamps]
    assert results == results2, "FAIL: детерминизм нарушен после reset()"
    print("  ✓ Детерминизм после reset() подтверждён")

    # Проверка что float не используется
    for r in results:
        for v in r.values():
            assert isinstance(v, int), f"FAIL: обнаружен не-int: {type(v)} = {v}"
    print("  ✓ Все значения int — float не используется")

    print(f"\n  Обработано событий: {engine.events_processed}")
    print("\n=== PASSED ===")
