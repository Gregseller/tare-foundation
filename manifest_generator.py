"""
manifest_generator.py — Генератор TARE_MANIFEST.toml
Читает PROJECT_MAP.md + RoadMap.txt и через LLM API
генерирует машиночитаемый манифест проекта.

Запуск:
    python manifest_generator.py
    python manifest_generator.py --model deepseek --max-tokens 16000
    python manifest_generator.py --model claude --dry-run
"""

import os
import sys
import argparse
import re
from datetime import datetime

# ==========================
# CLI
# ==========================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Генератор TARE_MANIFEST.toml из PROJECT_MAP.md"
    )
    parser.add_argument("--map",     default="PROJECT_MAP.md",
                        help="Путь к PROJECT_MAP.md")
    parser.add_argument("--road",    default="RoadMap.txt",
                        help="Путь к RoadMap.txt (опционально)")
    parser.add_argument("--out",     default="TARE_MANIFEST.toml",
                        help="Выходной файл")
    parser.add_argument("--model",   default="claude",
                        choices=["claude", "gemini", "deepseek"])
    parser.add_argument("--max-tokens", type=int, default=16000,
                        help="Максимум токенов в ответе (по умолч. 16000)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Показать промпт, не вызывать API")
    
    return parser.parse_args()


# ==========================
# ИМПОРТ МОДЕЛИ С ПОДДЕРЖКОЙ MAX_TOKENS
# ==========================

def import_model_with_tokens(backend: str, max_tokens: int):
    """
    Импортирует ask_model и оборачивает её, чтобы передавать max_tokens,
    если оригинальная функция поддерживает этот параметр.
    """
    backends = {
        "claude":   "ai_ask_Claude",
        "gemini":   "ai_ask_Gemini",
        "deepseek": "ai_ask_DeepSeek",
    }
    try:
        module = __import__(backends[backend])
        original_ask = module.ask_model
    except ImportError as e:
        print(f"❌ Не удалось импортировать {backends[backend]}: {e}")
        sys.exit(1)

    # Пробуем вызвать с max_tokens, если не получается — вызываем без
    def wrapped(prompt: str) -> str:
        try:
            # Пытаемся передать max_tokens
            return original_ask(prompt, max_tokens=max_tokens)
        except TypeError:
            # Если функция не принимает max_tokens, вызываем как есть
            return original_ask(prompt)
    return wrapped


# ==========================
# ЧТЕНИЕ ИСХОДНЫХ ФАЙЛОВ
# ==========================

def read_file_safe(path: str) -> str:
    if not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ==========================
# ПРОМПТ
# ==========================

PROMPT_TEMPLATE = """
Ты архитектор проекта TARE (Tick-Level Algorithmic Research Environment).

Ниже — PROJECT_MAP.md и RoadMap.txt проекта.
Твоя задача: на основе этих документов сгенерировать TARE_MANIFEST.toml —
машиночитаемый манифест всех модулей проекта.

=== PROJECT_MAP.md ===
{project_map}

=== RoadMap.txt ===
{roadmap}

=== ТЕКУЩАЯ СТРУКТУРА ПРОЕКТА ===
{current_structure}

=== ПРАВИЛА ГЕНЕРАЦИИ МАНИФЕСТА ===

1. Каждый модуль описывается блоком [[module]]
2. Обязательные поля каждого модуля:
   - name        : короткое имя (snake_case, без .py)
   - path        : путь к файлу относительно корня проекта
   - test        : путь к тест-файлу
   - phase       : номер фазы (1-9) из RoadMap
   - status      : "done" | "stub" | "missing"
                   done    = файл существует И тесты зелёные
                   stub    = файл существует но только заглушка
                   missing = файл отсутствует
   - depends_on  : список имён модулей от которых зависит этот
   - role        : одна строка — что делает модуль
   - interface   : главный класс/функция и сигнатура
   - tare_rules  : список правил TARE которые должны соблюдаться
                   ("only_int", "no_random", "stdlib_only", "deterministic")

3. Статус определяй по текущей структуре:
   - time_engine  → status = "done" (уже реализован и протестирован)
   - tick_reader  → status = "done" (уже реализован и протестирован)
   - остальное Phase 1 → status = "stub" если файл есть, "missing" если нет
   - Phase 2+ → status = "missing"

4. depends_on — только прямые зависимости, не транзитивные

5. В конце добавь секцию [project]:
   name        = "TARE"
   version     = "0.1.0"
   phase_current = 1
   generated_at = (текущая дата)

6. Добавь секцию [[phase]] для каждой фазы из RoadMap:
   number      = 1
   name        = "Foundation"
   goal        = "Deterministic data + time + replay"
   status      = "in_progress" | "pending"

=== ФОРМАТ ВЫВОДА ===
Верни ТОЛЬКО валидный TOML файл.
Никаких объяснений до или после.
Никаких markdown-блоков (``` или ```toml).
Только чистый TOML который можно сразу сохранить в файл.
"""


def build_prompt(project_map: str, roadmap: str,
                 current_structure: str) -> str:
    return PROMPT_TEMPLATE.format(
        project_map=project_map,
        roadmap=roadmap,
        current_structure=current_structure,
    )


# ==========================
# СКАНИРОВАНИЕ СТРУКТУРЫ
# ==========================

def scan_project_structure() -> str:
    """Сканирует текущую структуру проекта для контекста."""
    lines = ["Существующие файлы:"]
    exclude = {"__pycache__", ".venv", "venv", ".git",
               "debug_workspace", "node_modules", "ascii"}

    for root, dirs, files in os.walk("."):
        dirs[:] = sorted(d for d in dirs if d not in exclude)
        for fname in sorted(files):
            if fname.endswith(".py") or fname.endswith(".toml") \
               or fname.endswith(".md") or fname.endswith(".txt"):
                fpath = os.path.join(root, fname)
                size = os.path.getsize(fpath)
                tag = ""
                if fname.endswith(".py"):
                    if size < 500:
                        tag = "  ← заглушка (мало кода)"
                    elif size > 5000:
                        tag = "  ← полная реализация"
                lines.append(f"  {fpath}  ({size} bytes){tag}")
    return "\n".join(lines)


# ==========================
# ВАЛИДАЦИЯ TOML
# ==========================

def validate_toml(content: str) -> tuple[bool, str]:
    """Проверяет что ответ модели — валидный TOML."""
    try:
        import tomllib
        tomllib.loads(content)
        return True, ""
    except ImportError:
        pass
    try:
        import tomli
        tomli.loads(content)
        return True, ""
    except ImportError:
        pass

    # Базовая проверка без библиотеки
    required_patterns = [
        r"\[\[module\]\]",
        r'name\s*=\s*"',
        r'path\s*=\s*"',
        r'phase\s*=\s*\d',
        r'status\s*=\s*"',
        r"\[project\]",
    ]
    missing = []
    for pattern in required_patterns:
        if not re.search(pattern, content):
            missing.append(pattern)
    if missing:
        return False, f"Отсутствуют паттерны: {missing}"
    return True, ""


# ==========================
# ОЧИСТКА ОТВЕТА
# ==========================

def clean_response(response: str) -> str:
    """Убирает markdown-обёртку если модель её добавила."""
    response = response.strip()
    if response.startswith("```"):
        lines = response.split("\n")
        lines = lines[1:]  # убираем ```toml или ```
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        response = "\n".join(lines)
    return response.strip()


# ==========================
# ОСНОВНОЙ ПРОЦЕСС
# ==========================

def main():
    args = parse_args()

    print("=" * 60)
    print("TARE — Manifest Generator")
    print("=" * 60)

    project_map = read_file_safe(args.map)
    if not project_map:
        print(f"❌ {args.map} не найден!")
        sys.exit(1)
    print(f"✓ Прочитан {args.map} ({len(project_map)} символов)")

    roadmap = read_file_safe(args.road)
    if roadmap:
        print(f"✓ Прочитан {args.road} ({len(roadmap)} символов)")
    else:
        print(f"  [INFO] {args.road} не найден — продолжаем без него")

    structure = scan_project_structure()
    print(f"✓ Структура проекта просканирована")

    prompt = build_prompt(project_map, roadmap, structure)
    print(f"✓ Промпт готов ({len(prompt)} символов)")

    if args.dry_run:
        print("\n=== ПРОМПТ (dry-run) ===")
        print(prompt[:2000])
        print("\n... (обрезано для dry-run)")
        print("\n[dry-run] API не вызывался. Убери --dry-run для генерации.")
        return

    print(f"\n[→] Отправка в {args.model.upper()} (max_tokens={args.max_tokens})...")
    ask_model = import_model_with_tokens(args.model, args.max_tokens)
    response = ask_model(prompt)

    if not response:
        print("❌ Пустой ответ от модели.")
        sys.exit(1)

    print(f"✓ Получен ответ ({len(response)} символов)")

    with open("manifest_raw_response.log", "w", encoding="utf-8") as f:
        f.write(response)
    print(f"  📁 Сырой ответ → manifest_raw_response.log")

    toml_content = clean_response(response)

    ok, error = validate_toml(toml_content)
    if not ok:
        print(f"⚠️  TOML может быть невалидным: {error}")
        print("   Проверь manifest_raw_response.log")
    else:
        print("✓ TOML валиден")

    header = (
        f"# TARE_MANIFEST.toml\n"
        f"# Сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        f"# Источник: {args.map}\n"
        f"# Модель: {args.model.upper()}\n"
        f"# НЕ РЕДАКТИРУЙ ВРУЧНУЮ статус 'done' — он обновляется дебагером\n\n"
    )

    final_content = header + toml_content

    if os.path.exists(args.out):
        backup = args.out.replace(".toml", "_backup.toml")
        os.rename(args.out, backup)
        print(f"  📁 Старый манифест → {backup}")

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(final_content)

    print(f"\n✅ Манифест записан: {args.out}")
    print(f"\nСледующий шаг:")
    print(f"  python auto_debug_tare.py --scaffold")


if __name__ == "__main__":
    main()



# """
# manifest_generator.py — Генератор TARE_MANIFEST.toml
# Читает PROJECT_MAP.md + RoadMap.txt и через Claude API
# генерирует машиночитаемый манифест проекта.

# Запуск:
#     python manifest_generator.py
#     python manifest_generator.py --map PROJECT_MAP.md --out TARE_MANIFEST.toml
#     python manifest_generator.py --dry-run   # показать промпт без вызова API
# """

# import os
# import sys
# import argparse
# import re
# from datetime import datetime


# # ==========================
# # CLI
# # ==========================

# def parse_args():
#     parser = argparse.ArgumentParser(
#         description="Генератор TARE_MANIFEST.toml из PROJECT_MAP.md"
#     )
#     parser.add_argument("--map",     default="PROJECT_MAP.md",
#                         help="Путь к PROJECT_MAP.md")
#     parser.add_argument("--road",    default="RoadMap.txt",
#                         help="Путь к RoadMap.txt (опционально)")
#     parser.add_argument("--out",     default="TARE_MANIFEST.toml",
#                         help="Выходной файл")
#     parser.add_argument("--model",   default="claude",
#                         choices=["claude", "gemini", "deepseek"])
#     parser.add_argument("--dry-run", action="store_true",
#                         help="Показать промпт, не вызывать API")
#     return parser.parse_args()


# # ==========================
# # ИМПОРТ МОДЕЛИ
# # ==========================

# def import_model(backend: str):
#     backends = {
#         "claude":   "ai_ask_Claude",
#         "gemini":   "ai_ask_Gemini",
#         "deepseek": "ai_ask_DeepSeek",
#     }
#     try:
#         module = __import__(backends[backend])
#         return module.ask_model
#     except ImportError as e:
#         print(f"❌ Не удалось импортировать {backends[backend]}: {e}")
#         sys.exit(1)


# # ==========================
# # ЧТЕНИЕ ИСХОДНЫХ ФАЙЛОВ
# # ==========================

# def read_file_safe(path: str) -> str:
#     if not os.path.exists(path):
#         return ""
#     with open(path, "r", encoding="utf-8") as f:
#         return f.read()


# # ==========================
# # ПРОМПТ
# # ==========================

# PROMPT_TEMPLATE = """
# Ты архитектор проекта TARE (Tick-Level Algorithmic Research Environment).

# Ниже — PROJECT_MAP.md и RoadMap.txt проекта.
# Твоя задача: на основе этих документов сгенерировать TARE_MANIFEST.toml —
# машиночитаемый манифест всех модулей проекта.

# === PROJECT_MAP.md ===
# {project_map}

# === RoadMap.txt ===
# {roadmap}

# === ТЕКУЩАЯ СТРУКТУРА ПРОЕКТА ===
# {current_structure}

# === ПРАВИЛА ГЕНЕРАЦИИ МАНИФЕСТА ===

# 1. Каждый модуль описывается блоком [[module]]
# 2. Обязательные поля каждого модуля:
#    - name        : короткое имя (snake_case, без .py)
#    - path        : путь к файлу относительно корня проекта
#    - test        : путь к тест-файлу
#    - phase       : номер фазы (1-9) из RoadMap
#    - status      : "done" | "stub" | "missing"
#                    done    = файл существует И тесты зелёные
#                    stub    = файл существует но только заглушка
#                    missing = файл отсутствует
#    - depends_on  : список имён модулей от которых зависит этот
#    - role        : одна строка — что делает модуль
#    - interface   : главный класс/функция и сигнатура
#    - tare_rules  : список правил TARE которые должны соблюдаться
#                    ("only_int", "no_random", "stdlib_only", "deterministic")

# 3. Статус определяй по текущей структуре:
#    - time_engine  → status = "done" (уже реализован и протестирован)
#    - tick_reader  → status = "done" (уже реализован и протестирован)
#    - остальное Phase 1 → status = "stub" если файл есть, "missing" если нет
#    - Phase 2+ → status = "missing"

# 4. depends_on — только прямые зависимости, не транзитивные

# 5. В конце добавь секцию [project]:
#    name        = "TARE"
#    version     = "0.1.0"
#    phase_current = 1
#    generated_at = (текущая дата)

# 6. Добавь секцию [[phase]] для каждой фазы из RoadMap:
#    number      = 1
#    name        = "Foundation"
#    goal        = "Deterministic data + time + replay"
#    status      = "in_progress" | "pending"

# === ФОРМАТ ВЫВОДА ===
# Верни ТОЛЬКО валидный TOML файл.
# Никаких объяснений до или после.
# Никаких markdown-блоков (``` или ```toml).
# Только чистый TOML который можно сразу сохранить в файл.
# """


# def build_prompt(project_map: str, roadmap: str,
#                  current_structure: str) -> str:
#     return PROMPT_TEMPLATE.format(
#         project_map=project_map,
#         roadmap=roadmap,
#         current_structure=current_structure,
#     )


# # ==========================
# # СКАНИРОВАНИЕ СТРУКТУРЫ
# # ==========================

# def scan_project_structure() -> str:
#     """Сканирует текущую структуру проекта для контекста."""
#     lines = ["Существующие файлы:"]
#     exclude = {"__pycache__", ".venv", "venv", ".git",
#                "debug_workspace", "node_modules", "ascii"}

#     for root, dirs, files in os.walk("."):
#         dirs[:] = sorted(d for d in dirs if d not in exclude)
#         for fname in sorted(files):
#             if fname.endswith(".py") or fname.endswith(".toml") \
#                or fname.endswith(".md") or fname.endswith(".txt"):
#                 fpath = os.path.join(root, fname)
#                 size = os.path.getsize(fpath)
#                 # Помечаем заглушки (маленькие файлы)
#                 tag = ""
#                 if fname.endswith(".py"):
#                     if size < 500:
#                         tag = "  ← заглушка (мало кода)"
#                     elif size > 5000:
#                         tag = "  ← полная реализация"
#                 lines.append(f"  {fpath}  ({size} bytes){tag}")
#     return "\n".join(lines)


# # ==========================
# # ВАЛИДАЦИЯ TOML
# # ==========================

# def validate_toml(content: str) -> tuple[bool, str]:
#     """Проверяет что ответ модели — валидный TOML."""
#     # Python 3.11+ имеет tomllib в stdlib
#     try:
#         import tomllib
#         tomllib.loads(content)
#         return True, ""
#     except ImportError:
#         pass

#     # Python 3.10 и ниже — пробуем tomli
#     try:
#         import tomli
#         tomli.loads(content)
#         return True, ""
#     except ImportError:
#         pass

#     # Базовая проверка без библиотеки
#     required_patterns = [
#         r"\[\[module\]\]",
#         r'name\s*=\s*"',
#         r'path\s*=\s*"',
#         r'phase\s*=\s*\d',
#         r'status\s*=\s*"',
#         r"\[project\]",
#     ]
#     missing = []
#     for pattern in required_patterns:
#         if not re.search(pattern, content):
#             missing.append(pattern)

#     if missing:
#         return False, f"Отсутствуют паттерны: {missing}"
#     return True, ""


# # ==========================
# # ОЧИСТКА ОТВЕТА
# # ==========================

# def clean_response(response: str) -> str:
#     """Убирает markdown-обёртку если модель её добавила."""
#     response = response.strip()

#     # Убираем ```toml ... ``` или ``` ... ```
#     if response.startswith("```"):
#         lines = response.split("\n")
#         # Убираем первую строку (```toml или ```)
#         lines = lines[1:]
#         # Убираем последнюю строку если это ```
#         if lines and lines[-1].strip() == "```":
#             lines = lines[:-1]
#         response = "\n".join(lines)

#     return response.strip()


# # ==========================
# # ОСНОВНОЙ ПРОЦЕСС
# # ==========================

# def main():
#     args = parse_args()

#     print("=" * 60)
#     print("TARE — Manifest Generator")
#     print("=" * 60)

#     # Читаем исходники
#     project_map = read_file_safe(args.map)
#     if not project_map:
#         print(f"❌ {args.map} не найден!")
#         sys.exit(1)
#     print(f"✓ Прочитан {args.map} ({len(project_map)} символов)")

#     roadmap = read_file_safe(args.road)
#     if roadmap:
#         print(f"✓ Прочитан {args.road} ({len(roadmap)} символов)")
#     else:
#         print(f"  [INFO] {args.road} не найден — продолжаем без него")

#     # Сканируем структуру
#     structure = scan_project_structure()
#     print(f"✓ Структура проекта просканирована")

#     # Строим промпт
#     prompt = build_prompt(project_map, roadmap, structure)
#     print(f"✓ Промпт готов ({len(prompt)} символов)")

#     if args.dry_run:
#         print("\n=== ПРОМПТ (dry-run) ===")
#         print(prompt[:2000])
#         print("\n... (обрезано для dry-run)")
#         print("\n[dry-run] API не вызывался. Убери --dry-run для генерации.")
#         return

#     # Вызов модели
#     print(f"\n[→] Отправка в {args.model.upper()}...")
#     ask_model = import_model(args.model)
#     response = ask_model(prompt)

#     if not response:
#         print("❌ Пустой ответ от модели.")
#         sys.exit(1)

#     print(f"✓ Получен ответ ({len(response)} символов)")

#     # Сохраняем сырой ответ для диагностики
#     with open("manifest_raw_response.log", "w", encoding="utf-8") as f:
#         f.write(response)
#     print(f"  📁 Сырой ответ → manifest_raw_response.log")

#     # Чистим
#     toml_content = clean_response(response)

#     # Валидируем
#     ok, error = validate_toml(toml_content)
#     if not ok:
#         print(f"⚠️  TOML может быть невалидным: {error}")
#         print("   Проверь manifest_raw_response.log")
#         # Всё равно сохраняем — пусть пользователь решает
#     else:
#         print("✓ TOML валиден")

#     # Добавляем заголовок
#     header = (
#         f"# TARE_MANIFEST.toml\n"
#         f"# Сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
#         f"# Источник: {args.map}\n"
#         f"# Модель: {args.model.upper()}\n"
#         f"# НЕ РЕДАКТИРУЙ ВРУЧНУЮ статус 'done' — он обновляется дебагером\n\n"
#     )

#     final_content = header + toml_content

#     # Бэкап если файл уже существует
#     if os.path.exists(args.out):
#         backup = args.out.replace(".toml", "_backup.toml")
#         os.rename(args.out, backup)
#         print(f"  📁 Старый манифест → {backup}")

#     # Записываем
#     with open(args.out, "w", encoding="utf-8") as f:
#         f.write(final_content)

#     print(f"\n✅ Манифест записан: {args.out}")
#     print(f"\nСледующий шаг:")
#     print(f"  python auto_debug_tare.py --scaffold")


# if __name__ == "__main__":
#     main()