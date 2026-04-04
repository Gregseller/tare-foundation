# TARE — AI Developer Context
## Вставляй этот файл в начало каждого запроса к DeepSeek

---

## Что такое TARE

TARE (Tick-Level Algorithmic Research Environment) — детерминированная
исследовательская среда для работы с биржевыми данными на уровне тиков.
Язык: Python 3.12. Только стандартная библиотека.

---

## Три закона TARE (нарушение недопустимо)

1. **Только int** — никакого float нигде и никогда внутри системы
2. **Никакой случайности** — ни random, ни uuid, ни time.time()
3. **Детерминизм** — одни входные данные → всегда одинаковый результат

---

## Структура проекта

```
tare/
  time_engine/        ← Phase 1: TimeEngine (done)
  tick_data_engine/   ← Phase 1: TickReader, TickCleaner, JitterCorrector (done)
  memory/             ← Phase 1: RingBuffer, Chunking, MmapLoader (done)
  snapshot/           ← Phase 1: SnapshotV1, Replay (done)
  utils/              ← Phase 1: assertions (done)
  microstructure/     ← Phase 2: LODEngine, LatencyModel, SlippageEngine,
                                  PartialFills, QueuePosition, FXGaps,
                                  SwapRollover (done)
  execution/          ← Phase 3: ExecutionEngine, PortfolioEngine
  strategy/           ← Phase 4: StrategyDAG
  validation/         ← Phase 3+5: AdequacyV1, AdequacyV2
  research/           ← Phase 6: HypothesisGenerator, BatchTesting
  live/               ← Phase 8: FIXConnector, WebSocketConnector
  evolution/          ← Phase 9: DriftDetection, RegimeDetection
tests/                ← все тесты здесь
```

---

## Формат ответа (строго)

Когда тебя просят написать модуль — возвращай ТОЛЬКО Python код.
Никаких объяснений, никаких markdown блоков (``` или ```python).
Только чистый код который можно сразу сохранить в файл.

Когда тебя просят написать тесты — то же самое.

---

## Шаблон модуля (пример правильного кода)

```python
"""
module_name.py — Описание модуля для TARE.
Phase: X
"""

class ClassName:
    """Одна строка описания."""

    @staticmethod
    def method_name(param: int) -> int:
        """Что делает метод."""
        if not isinstance(param, int):
            raise ValueError("param must be int")
        if param <= 0:
            raise ValueError("param must be positive")
        return param * 2  # только int операции
```

---

## Шаблон тестов (пример правильного теста)

```python
import unittest
from tare.module.class_name import ClassName

class TestClassName(unittest.TestCase):
    def setUp(self):
        self.obj = ClassName()

    def test_normal_case(self):
        result = self.obj.method_name(5)
        self.assertIsInstance(result, int)
        self.assertEqual(result, 10)

    def test_float_input_raises(self):
        with self.assertRaises(ValueError):
            self.obj.method_name(5.0)

    def test_determinism(self):
        r1 = self.obj.method_name(5)
        r2 = self.obj.method_name(5)
        self.assertEqual(r1, r2)

if __name__ == "__main__":
    unittest.main()
```

---

## Что уже реализовано (не переписывать)

- `TimeEngine` — три временных домена, монотонный счётчик
- `TickReader` — читает CSV Dukascopy формата `YYYYMMDD HHMMSSmmm,bid,ask,vol`
- `TickCleaner` — удаляет дубли, невалидные тики
- `JitterCorrector` — коррекция временных артефактов
- `LODEngine` — уровни детализации рынка (LOD1/2/3)
- `LatencyModel` — модель задержки исполнения
- `SlippageEngine` — расчёт проскальзывания
- `PartialFills` — частичное исполнение ордеров
- `QueuePosition` — позиция в очереди на бирже
- `FXGaps` — ценовые гэпы
- `SwapRollover` — стоимость переноса позиции

---

## Данные проекта

FX тики Dukascopy ASCII формат:
```
20160401 000001277,0.793750,0.793810,0
YYYYMMDD HHMMSSmmm,bid,ask,volume
```
Пары: EURGBP, EURUSD, GBPUSD, USDJPY
Период: 2016–2025, ~2M тиков/месяц на пару

Конвертация цен: float → int умножением на 100_000
(0.793750 → 79375 пунктов)

---

## Текущий статус

Phase 1: ✅ done (354 теста)
Phase 2: ✅ done (410 тестов суммарно)
Phase 3: 🔄 в работе — ExecutionEngine, PortfolioEngine
