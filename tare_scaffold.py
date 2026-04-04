"""
tare_scaffold.py — Генератор структуры проекта TARE
Запускается один раз. Создаёт папки, __init__.py и скелеты модулей.

Запуск:
    python tare_scaffold.py
    python tare_scaffold.py --root /путь/к/проекту
    python tare_scaffold.py --dry-run   # показать что будет создано, без записи
"""

import os
import argparse
import sys
from datetime import datetime


# ==========================
# СТРУКТУРА ПРОЕКТА
# ==========================

# Формат: (относительный путь, содержимое файла или None для папок)
PROJECT_STRUCTURE = [

    # Корень пакета
    ("tare/__init__.py", '"""TARE — Tick-Level Algorithmic Research Environment."""\n\n__version__ = "0.1.0"\n'),

    # ── time_engine ──────────────────────────────────────────────────
    ("tare/time_engine/__init__.py",
     '"""TimeEngine — единственный источник истины о времени в симуляции."""\n\nfrom .time_engine import TimeEngine\n\n__all__ = ["TimeEngine"]\n'),

    ("tare/time_engine/time_engine.py", None),        # уже создан — не перезаписываем
    ("tare/time_engine/test_time_engine.py", None),   # уже создан — не перезаписываем

    # ── tick_data_engine ─────────────────────────────────────────────
    ("tare/tick_data_engine/__init__.py",
     '"""TickDataEngine — ингест, очистка, сортировка тиковых данных."""\n'),

    ("tare/tick_data_engine/tick_reader.py",
     '"""\ntick_reader.py — Чтение бинарных/CSV потоков тиковых данных.\n\nТочка входа для сырых данных. Отвечает только за чтение,\nникакой очистки здесь.\n"""\n\n\nclass TickReader:\n    """Заглушка. Реализация — Phase 1."""\n\n    def __init__(self, source: str):\n        self.source = source\n\n    def read(self):\n        raise NotImplementedError("TickReader.read() — Phase 1")\n'),

    ("tare/tick_data_engine/tick_cleaner.py",
     '"""\ntick_cleaner.py — Очистка и фильтрация тиковых данных.\n\nУбирает дубли, NaN, выбросы. Не меняет порядок.\n"""\n\n\nclass TickCleaner:\n    """Заглушка. Реализация — Phase 1."""\n\n    def clean(self, ticks):\n        raise NotImplementedError("TickCleaner.clean() — Phase 1")\n'),

    ("tare/tick_data_engine/jitter_corrector.py",
     '"""\njitter_corrector.py — Коррекция временных артефактов (jitter).\n\nИсправляет рассинхронизацию timestamps без изменения данных.\n"""\n\n\nclass JitterCorrector:\n    """Заглушка. Реализация — Phase 1."""\n\n    def correct(self, ticks):\n        raise NotImplementedError("JitterCorrector.correct() — Phase 1")\n'),

    # ── snapshot ─────────────────────────────────────────────────────
    ("tare/snapshot/__init__.py",
     '"""SnapshotEngine — детерминированные снимки состояния."""\n'),

    ("tare/snapshot/snapshot_v1.py",
     '"""\nsnapshot_v1.py — Структура исторического снимка (v1).\n\nХранит состояние рынка в конкретный момент времени.\n"""\n\n\nclass SnapshotV1:\n    """Заглушка. Реализация — Phase 1."""\n\n    def __init__(self, simulation_time: int):\n        self.simulation_time = simulation_time\n\n    def capture(self, state: dict):\n        raise NotImplementedError("SnapshotV1.capture() — Phase 1")\n'),

    ("tare/snapshot/replay.py",
     '"""\nreplay.py — Воспроизведение исторических потоков данных.\n\nДетерминированный replay: один seed → один результат всегда.\n"""\n\n\nclass ReplayEngine:\n    """Заглушка. Реализация — Phase 1."""\n\n    def replay(self, snapshot):\n        raise NotImplementedError("ReplayEngine.replay() — Phase 1")\n'),

    # ── memory ───────────────────────────────────────────────────────
    ("tare/memory/__init__.py",
     '"""Memory — высокопроизводительные структуры данных."""\n'),

    ("tare/memory/ring_buffer.py",
     '"""\nring_buffer.py — Циклический буфер фиксированного размера.\n\nO(1) запись и чтение. Без аллокаций в горячем пути.\n"""\n\n\nclass RingBuffer:\n    """Заглушка. Реализация — Phase 1."""\n\n    def __init__(self, capacity: int):\n        self.capacity = capacity\n\n    def push(self, item):\n        raise NotImplementedError("RingBuffer.push() — Phase 1")\n\n    def pop(self):\n        raise NotImplementedError("RingBuffer.pop() — Phase 1")\n'),

    ("tare/memory/chunking.py",
     '"""\nchunking.py — Разбиение больших потоков данных на чанки.\n\nПозволяет обрабатывать данные, не загружая всё в память.\n"""\n\n\nclass ChunkIterator:\n    """Заглушка. Реализация — Phase 1."""\n\n    def __init__(self, data, chunk_size: int):\n        self.data = data\n        self.chunk_size = chunk_size\n\n    def __iter__(self):\n        raise NotImplementedError("ChunkIterator — Phase 1")\n'),

    ("tare/memory/mmap_loader.py",
     '"""\nmmap_loader.py — Memory-mapped файлы для больших датасетов.\n\nПозволяет читать терабайтные файлы без загрузки в RAM.\n"""\n\n\nclass MmapLoader:\n    """Заглушка. Реализация — Phase 1."""\n\n    def __init__(self, filepath: str):\n        self.filepath = filepath\n\n    def load(self):\n        raise NotImplementedError("MmapLoader.load() — Phase 1")\n'),

    # ── utils ─────────────────────────────────────────────────────────
    ("tare/utils/__init__.py", '"""Утилиты TARE."""\n'),

    ("tare/utils/assertions.py",
     '"""\nassertions.py — Детерминистические проверки инвариантов.\n\nИспользуется во всех модулях для контроля корректности данных.\n"""\n\n\ndef assert_int(value, name: str = "value") -> None:\n    """Гарантирует что value — int. Иначе TypeError."""\n    if not isinstance(value, int):\n        raise TypeError(f"{name} должен быть int, получен {type(value).__name__}: {value!r}")\n\n\ndef assert_positive(value: int, name: str = "value") -> None:\n    """Гарантирует что value > 0."""\n    assert_int(value, name)\n    if value <= 0:\n        raise ValueError(f"{name} должен быть > 0, получен: {value}")\n\n\ndef assert_non_negative(value: int, name: str = "value") -> None:\n    """Гарантирует что value >= 0."""\n    assert_int(value, name)\n    if value < 0:\n        raise ValueError(f"{name} должен быть >= 0, получен: {value}")\n'),

    # ── tests (верхний уровень) ───────────────────────────────────────
    ("tests/__init__.py", ""),

    ("tests/test_smoke.py",
     '"""\ntest_smoke.py — Дымовые тесты: импорт и базовая связность.\n\nЕсли это падает — что-то сломано фундаментально.\n"""\nimport unittest\n\n\nclass TestImports(unittest.TestCase):\n\n    def test_import_tare(self):\n        import tare\n        self.assertEqual(tare.__version__, "0.1.0")\n\n    def test_import_time_engine(self):\n        from tare.time_engine import TimeEngine\n        self.assertIsNotNone(TimeEngine)\n\n    def test_time_engine_instantiation(self):\n        from tare.time_engine import TimeEngine\n        engine = TimeEngine(base_latency_ns=100)\n        self.assertEqual(engine.base_latency_ns, 100)\n\n\nif __name__ == "__main__":\n    unittest.main(verbosity=2)\n'),

    # ── CLI ───────────────────────────────────────────────────────────
    ("tare_cli.py",
     '"""\ntare_cli.py — Точка входа командной строки.\n\nПока заглушка. Phase 1.\n"""\nimport sys\n\n\ndef main():\n    print("TARE CLI v0.1 — Phase 1 in progress")\n    return 0\n\n\nif __name__ == "__main__":\n    sys.exit(main())\n'),

    # ── README ────────────────────────────────────────────────────────
    ("README.md",
     f"# TARE — Tick-Level Algorithmic Research Environment\n\n"
     f"Создан: {datetime.now().strftime('%Y-%m-%d')}\n\n"
     "## Структура\n\n"
     "```\n"
     "tare/\n"
     "  time_engine/     ← Phase 1: единственный источник истины о времени\n"
     "  tick_data_engine/ ← Phase 1: ингест и очистка тиков\n"
     "  snapshot/        ← Phase 1: детерминированные снимки\n"
     "  memory/          ← Phase 1: высокопроизводительные структуры\n"
     "  utils/           ← общие утилиты\n"
     "tests/             ← юнит и дымовые тесты\n"
     "tare_cli.py        ← CLI точка входа\n"
     "tare_scaffold.py   ← этот файл (запускается один раз)\n"
     "auto_debug_tare.py ← автодебагер\n"
     "```\n\n"
     "## Принципы\n\n"
     "- Только `int`, никакого `float`\n"
     "- Никакой случайности\n"
     "- Детерминизм: одни данные → один результат всегда\n"
     "- Только стандартная библиотека Python\n"
     "- Один модуль — одна ответственность\n\n"
     "## Запуск тестов\n\n"
     "```bash\n"
     "python -m pytest tests/ -v\n"
     "python -m pytest tare/time_engine/test_time_engine.py -v\n"
     "```\n"),
]


# ==========================
# СОЗДАНИЕ СТРУКТУРЫ
# ==========================

def create_structure(root: str, dry_run: bool = False):
    created_dirs  = []
    created_files = []
    skipped_files = []
    errors        = []

    for rel_path, content in PROJECT_STRUCTURE:
        full_path = os.path.join(root, rel_path)
        dir_path  = os.path.dirname(full_path)

        # Создаём директорию
        if not os.path.exists(dir_path):
            if dry_run:
                print(f"  [DIR]  {dir_path}")
            else:
                try:
                    os.makedirs(dir_path, exist_ok=True)
                    created_dirs.append(dir_path)
                except Exception as e:
                    errors.append(f"Не удалось создать папку {dir_path}: {e}")
                    continue

        # content=None → файл уже должен существовать, не трогаем
        if content is None:
            if os.path.exists(full_path):
                skipped_files.append(full_path)
                if dry_run:
                    print(f"  [SKIP] {full_path}  ← уже существует")
            else:
                print(f"  [WARN] {full_path} — помечен как существующий, но не найден!")
            continue

        # Не перезаписываем существующие файлы
        if os.path.exists(full_path):
            skipped_files.append(full_path)
            if dry_run:
                print(f"  [SKIP] {full_path}  ← уже существует")
            continue

        if dry_run:
            print(f"  [FILE] {full_path}")
        else:
            try:
                with open(full_path, "w", encoding="utf-8") as f:
                    f.write(content)
                created_files.append(full_path)
            except Exception as e:
                errors.append(f"Не удалось создать {full_path}: {e}")

    return created_dirs, created_files, skipped_files, errors


def print_tree(root: str):
    """Печатает дерево созданной структуры."""
    print(f"\n{root}/")
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted([d for d in dirnames
                               if d not in {"__pycache__", ".git", ".venv", "venv"}])
        level = dirpath.replace(root, "").count(os.sep)
        indent = "    " * level
        folder = os.path.basename(dirpath)
        if level > 0:
            print(f"{indent}├── {folder}/")
        for fname in sorted(filenames):
            sub = "    " * (level + 1)
            print(f"{sub}├── {fname}")


# ==========================
# CLI
# ==========================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Генератор структуры проекта TARE"
    )
    parser.add_argument(
        "--root", default=".",
        help="Корневая папка проекта (по умолчанию: текущая)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Показать что будет создано, без записи на диск"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    root = os.path.abspath(args.root)

    print("=" * 60)
    print("TARE — Scaffolder v1")
    print("=" * 60)
    print(f"Root:    {root}")
    print(f"Dry run: {args.dry_run}")
    print()

    if args.dry_run:
        print("Что будет создано:\n")

    dirs, files, skipped, errors = create_structure(root, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"\n✓ Папок создано:  {len(dirs)}")
        print(f"✓ Файлов создано: {len(files)}")
        print(f"  Пропущено:      {len(skipped)}  (уже существовали)")

        if errors:
            print(f"\n❌ Ошибки ({len(errors)}):")
            for e in errors:
                print(f"  {e}")
            sys.exit(1)

        print_tree(root)

        print("\n✅ Структура создана.")
        print("\nСледующий шаг:")
        print("  python -m pytest tests/ -v")


if __name__ == "__main__":
    main()
