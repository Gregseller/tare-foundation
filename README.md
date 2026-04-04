# TARE — Tick-Level Algorithmic Research Environment

Создан: 2026-04-02

## Структура

```
tare/
  time_engine/     ← Phase 1: единственный источник истины о времени
  tick_data_engine/ ← Phase 1: ингест и очистка тиков
  snapshot/        ← Phase 1: детерминированные снимки
  memory/          ← Phase 1: высокопроизводительные структуры
  utils/           ← общие утилиты
tests/             ← юнит и дымовые тесты
tare_cli.py        ← CLI точка входа
tare_scaffold.py   ← этот файл (запускается один раз)
auto_debug_tare.py ← автодебагер
```

## Принципы

- Только `int`, никакого `float`
- Никакой случайности
- Детерминизм: одни данные → один результат всегда
- Только стандартная библиотека Python
- Один модуль — одна ответственность

## Запуск тестов

```bash
python -m pytest tests/ -v
python -m pytest tare/time_engine/test_time_engine.py -v
```
