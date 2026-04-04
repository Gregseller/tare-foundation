"""
auto_debug_tare.py — Автодебагер TARE v2
Два режима:

  РЕЖИМ 1 — Дебаггинг (по умолчанию):
    python auto_debug_tare.py tare/time_engine/test_time_engine.py
    Запускает тесты → ловит ошибки → чинит через модель

  РЕЖИМ 2 — Scaffold (--scaffold):
    python auto_debug_tare.py --scaffold
    Читает TARE_MANIFEST.toml → находит missing/stub модули →
    генерирует их через модель в правильном порядке (depends_on граф)

  Флаги:
    --model   -m  : claude | gemini | deepseek (default: claude)
    --attempts -a : максимум попыток дебаггинга (default: 5)
    --scaffold    : режим генерации по манифесту
    --manifest    : путь к манифесту (default: TARE_MANIFEST.toml)
    --phase    -p : генерировать только модули этой фазы (scaffold режим)
    --dry-run     : показать план без генерации (scaffold режим)
    --verbose  -v : подробный вывод
"""

import os
import sys
import subprocess
import re
import argparse
import time
import platform
import heapq
from datetime import datetime


# ==========================
# RETRY С ЭКСПОНЕНЦИАЛЬНОЙ ЗАДЕРЖКОЙ
# ==========================

def call_with_retry(ask_func, prompt, max_retries=3, base_delay=1):
    """
    Вызывает ask_func(prompt) с повторными попытками при ошибках.
    Использует экспоненциальную задержку между попытками.
    """
    for attempt in range(max_retries + 1):
        try:
            response = ask_func(prompt)
            if response is not None:
                return response
            # Если ответ None, считаем ошибкой (но без конкретного кода)
            if attempt == max_retries:
                return None
            delay = base_delay * (2 ** attempt)
            print(f"  ⚠️ Пустой ответ, повтор через {delay:.1f} сек...")
            time.sleep(delay)
        except Exception as e:
            error_str = str(e)
            # Проверяем на 429 или другие временные ошибки
            if "429" in error_str or "rate_limit" in error_str.lower():
                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    print(f"  ⚠️ Rate limit (429), повтор через {delay:.1f} сек...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"  ❌ Превышено число попыток: {e}")
                    return None
            else:
                # Не временная ошибка — сразу выходим
                print(f"  ❌ Ошибка: {e}")
                return None
    return None


# ==========================
# ИМПОРТ МОДЕЛИ
# ==========================

def import_model(backend: str):
    backends = {
        "deepseek": "ai_ask_DeepSeek",
        "gemini":   "ai_ask_Gemini",
        "claude":   "ai_ask_Claude",
        "openai":   "ai_ask_OpenAI",
    }
    if backend not in backends:
        print(f"❌ Неизвестный бэкенд: {backend}")
        sys.exit(1)
    try:
        module = __import__(backends[backend])
        return module.ask_model
    except ImportError as e:
        print(f"❌ Не удалось импортировать {backends[backend]}: {e}")
        sys.exit(1)


# ==========================
# CLI
# ==========================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Автодебагер TARE v2 — debug + scaffold"
    )
    parser.add_argument("test_file", nargs="?",
                        help="Тест-файл для дебаггинга (режим 1)")
    parser.add_argument("--scaffold",  action="store_true",
                        help="Режим генерации модулей по манифесту")
    parser.add_argument("--manifest",  default="TARE_MANIFEST.toml",
                        help="Путь к TARE_MANIFEST.toml")
    parser.add_argument("--model",    "-m", default="claude",
                        choices=["deepseek", "gemini", "claude", "openai"])
    parser.add_argument("--attempts", "-a", type=int, default=5)
    parser.add_argument("--phase",    "-p", type=int, default=None,
                        help="Только модули этой фазы (scaffold)")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Показать план без генерации (scaffold)")
    parser.add_argument("--verbose",  "-v", action="store_true")
    return parser.parse_args()


# ==========================
# ЧТЕНИЕ МАНИФЕСТА
# ==========================

def load_manifest(path: str) -> dict:
    """
    Читает TARE_MANIFEST.toml.
    Возвращает dict с ключами 'project', 'phases', 'modules'.
    """
    if not os.path.exists(path):
        print(f"❌ Манифест не найден: {path}")
        print("   Запусти сначала: python manifest_generator.py")
        sys.exit(1)

    # Python 3.11+ — tomllib в stdlib
    try:
        import tomllib
        with open(path, "rb") as f:
            return tomllib.load(f)
    except ImportError:
        pass

    # # Python 3.10 и ниже — tomli
    # try:
    #     import tomli
    #     with open(path, "rb") as f:
    #         return tomli.load(f)
    # except ImportError:
    #     pass

    # Fallback: минимальный ручной парсер (только для нашего формата)
    return _parse_toml_minimal(path)


def _parse_toml_minimal(path: str) -> dict:
    """
    Минимальный парсер TOML для случая когда нет tomllib/tomli.
    Читает только [[module]] блоки и [project].
    """
    result = {"module": [], "phase": [], "project": {}}

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # Убираем комментарии
    lines = []
    for line in content.split("\n"):
        stripped = line.split("#")[0].rstrip()
        lines.append(stripped)
    content = "\n".join(lines)

    # Парсим [[module]] блоки
    module_blocks = re.split(r"\[\[module\]\]", content)[1:]
    for block in module_blocks:
        module = {}
        for line in block.split("\n"):
            line = line.strip()
            if not line or line.startswith("["):
                break
            m = re.match(r'(\w+)\s*=\s*"([^"]*)"', line)
            if m:
                module[m.group(1)] = m.group(2)
                continue
            m = re.match(r'(\w+)\s*=\s*(\d+)', line)
            if m:
                module[m.group(1)] = int(m.group(2))
                continue
            m = re.match(r'(\w+)\s*=\s*\[([^\]]*)\]', line)
            if m:
                items = re.findall(r'"([^"]*)"', m.group(2))
                module[m.group(1)] = items
                continue
        if module.get("name"):
            result["module"].append(module)

    # Парсим [[phase]] блоки
    phase_blocks = re.split(r"\[\[phase\]\]", content)[1:]
    for block in phase_blocks:
        phase = {}
        for line in block.split("\n"):
            line = line.strip()
            if not line or line.startswith("["):
                break
            m = re.match(r'(\w+)\s*=\s*"([^"]*)"', line)
            if m:
                phase[m.group(1)] = m.group(2)
                continue
            m = re.match(r'(\w+)\s*=\s*(\d+)', line)
            if m:
                phase[m.group(1)] = int(m.group(2))
        if phase:
            result["phase"].append(phase)

    # Парсим [project]
    project_match = re.search(r"\[project\](.*?)(?=\[|\Z)", content, re.DOTALL)
    if project_match:
        for line in project_match.group(1).split("\n"):
            line = line.strip()
            m = re.match(r'(\w+)\s*=\s*"([^"]*)"', line)
            if m:
                result["project"][m.group(1)] = m.group(2)
            m2 = re.match(r'(\w+)\s*=\s*(\d+)', line)
            if m2:
                result["project"][m2.group(1)] = int(m2.group(2))

    return result


# ==========================
# ТОПОЛОГИЧЕСКАЯ СОРТИРОВКА
# ==========================

def topological_sort(modules: list[dict]) -> list[dict]:
    """
    Сортирует модули в порядке depends_on (граф зависимостей).
    Модули без зависимостей идут первыми.
    Внутри одного уровня — сортировка по phase, потом по name.
    """
    name_to_module = {m["name"]: m for m in modules}
    visited = set()
    result  = []

    def visit(name: str):
        if name in visited:
            return
        visited.add(name)
        module = name_to_module.get(name)
        if not module:
            return
        for dep in module.get("depends_on", []):
            visit(dep)
        result.append(module)

    for module in sorted(modules,
                         key=lambda m: (m.get("phase", 99), m.get("name", ""))):
        visit(module["name"])

    return result


# ==========================
# ОБНОВЛЕНИЕ СТАТУСА В МАНИФЕСТЕ
# ==========================

def update_manifest_status(manifest_path: str, module_name: str,
                            new_status: str) -> None:
    """
    Обновляет status модуля в TOML файле.
    Простая замена строки — не трогает остальное.
    """
    with open(manifest_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Находим блок этого модуля и меняем его status
    # Ищем: name = "module_name" ... status = "old"
    pattern = re.compile(
        r'(name\s*=\s*"' + re.escape(module_name) + r'".*?status\s*=\s*)"[^"]*"',
        re.DOTALL
    )
    new_content = pattern.sub(
        lambda m: m.group(1) + f'"{new_status}"',
        content
    )

    if new_content != content:
        with open(manifest_path, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"  📝 Манифест обновлён: {module_name} → {new_status}")


# ==========================
# ПРОМПТ ДЛЯ ГЕНЕРАЦИИ МОДУЛЯ
# ==========================

TARE_RULES = """
=== ПРАВИЛА TARE (НАРУШЕНИЕ НЕДОПУСТИМО) ===
1. ТОЛЬКО int — никакого float нигде
2. НИКАКОЙ случайности — ни random, ни uuid, ни time.time()
3. ДЕТЕРМИНИЗМ — одни входные данные → всегда одинаковый результат
4. ТОЛЬКО стандартная библиотека Python
5. Генераторы для потоков данных — не загружать всё в память
6. Docstring для каждого метода
"""

TARE_FORMAT = """
=== ФОРМАТ ОТВЕТА (СТРОГО) ===
Верни два файла в этом формате:

=== FILE: {path} ===
(полный код модуля)
=== END FILE ===

=== FILE: {test} ===
(полный код тестов)
=== END FILE ===

Никаких объяснений до или после блоков FILE.
"""


def build_scaffold_prompt(module: dict, context: str) -> str:
    path = module.get("path", "")
    test = module.get("test", "")

    task = f"""
Ты разработчик проекта TARE (Tick-Level Algorithmic Research Environment).

Сгенерируй модуль по следующей спецификации:

=== МОДУЛЬ ===
Имя:        {module.get('name')}
Файл:       {path}
Фаза:       {module.get('phase')}
Роль:       {module.get('role', 'не указана')}
Интерфейс:  {module.get('interface', 'не указан')}
Зависит от: {module.get('depends_on', [])}
Правила:    {module.get('tare_rules', [])}
"""

    return task + TARE_RULES + TARE_FORMAT.format(
        path=path, test=test
    ) + f"\n\n{context}"


# ==========================
# ═══════════════════════════
# РЕЖИМ 1: ДЕБАГГИНГ
# ═══════════════════════════
# ==========================

def run_tests(test_path: str, verbose: bool = False) -> tuple[int, str]:
    pytest_cmd = [sys.executable, "-m", "pytest", test_path,
                  "-v", "--tb=short", "--no-header"]
    try:
        result = subprocess.run(
            pytest_cmd, capture_output=True, text=True,
            encoding="utf-8", timeout=120
        )
        output = result.stdout + "\n" + result.stderr
        if verbose:
            print(output)
        return result.returncode, output.strip()
    except FileNotFoundError:
        pass
    except subprocess.TimeoutExpired:
        return 1, "TIMEOUT: тесты не завершились за 120 секунд"

    # Fallback: unittest
    result = subprocess.run(
        [sys.executable, "-m", "unittest", test_path, "-v"],
        capture_output=True, text=True, encoding="utf-8", timeout=120
    )
    output = result.stdout + "\n" + result.stderr
    if verbose:
        print(output)
    return result.returncode, output.strip()


def check_syntax() -> tuple[bool, str]:
    exclude_dirs  = {"__pycache__", ".venv", "venv", "debug_workspace", ".git"}
    exclude_files = {
        "auto_debug_tare.py", "manifest_generator.py",
        "ai_ask_Gemini.py", "ai_ask_DeepSeek.py",
        "ai_ask_Claude.py", "ai_ask_OpenAI.py",
        "tare_scaffold.py",
    }
    errors = []
    for root, dirs, files in os.walk("."):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for fname in sorted(files):
            if not fname.endswith(".py") or fname in exclude_files:
                continue
            fpath = os.path.join(root, fname)
            result = subprocess.run(
                [sys.executable, "-m", "py_compile", fpath],
                capture_output=True, text=True
            )
            if result.returncode != 0:
                err = result.stderr.strip()
                print(f"  ✗ {fpath}: {err}")
                errors.append(f"{fpath}: {err}")
            else:
                print(f"  ✓ {fpath}")
    return (True, "") if not errors else (False, "\n".join(errors))


def check_determinism(test_path: str) -> tuple[bool, str]:
    print("[ДЕТЕРМИНИЗМ] Два прогона...")
    code1, out1 = run_tests(test_path)
    code2, out2 = run_tests(test_path)
    if code1 != code2:
        return False, f"Прогон 1: rc={code1}, Прогон 2: rc={code2}"

    def summary(output):
        for line in output.split("\n"):
            if "passed" in line or "failed" in line or "error" in line:
                return re.sub(r"\d+\.\d+s", "Xs", line)
        return ""

    s1, s2 = summary(out1), summary(out2)
    if s1 != s2:
        return False, f"Результаты различаются:\n  {s1}\n  {s2}"
    print(f"  ✓ Оба прогона: {s1.strip()}")
    return True, ""


def collect_context(test_path: str) -> str:
    lines = [
        f"Python: {sys.version}",
        f"Platform: {platform.platform()}",
        f"CWD: {os.getcwd()}",
        f"Time: {datetime.now().isoformat()}",
        f"Test file: {test_path}",
        "\n=== ФАЙЛЫ ПРОЕКТА TARE ===",
    ]
    exclude_dirs  = {"__pycache__", ".venv", "venv", "debug_workspace", ".git"}
    exclude_files = {
        "auto_debug_tare.py", "manifest_generator.py",
        "ai_ask_Gemini.py", "ai_ask_DeepSeek.py",
        "ai_ask_Claude.py", "ai_ask_OpenAI.py",
    }
    for root, dirs, files in os.walk("."):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        for fname in sorted(files):
            if not fname.endswith(".py") or fname in exclude_files:
                continue
            fpath = os.path.join(root, fname)
            rel = os.path.relpath(fpath)
            if not (rel.startswith("tare") or rel.startswith("tests")):
                continue
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    lines.append(f"\n===== FILE: {fpath} =====\n{f.read()}")
            except Exception as e:
                lines.append(f"\n===== FILE: {fpath} (ОШИБКА: {e}) =====")
    return "\n".join(lines)


def apply_patch(llm_response: str) -> bool:
    pattern = re.compile(
        r"={2,}\s*FILE:\s*(.+?)\s*={2,}\r?\n(.*?)(?:\r?\n={2,}\s*END FILE\s*={2,}|(?=\r?\n={2,}\s*FILE:|\Z))",
        re.DOTALL | re.IGNORECASE
    )
    matches = pattern.findall(llm_response)
    if not matches:
        print("  Нет патчей в ответе.")
        return False

    for filepath, new_code in matches:
        filepath = filepath.strip().strip('"\'')
        if not filepath:
            continue
        new_code = new_code.strip()
        for fence in ("```python", "```"):
            if new_code.startswith(fence):
                new_code = new_code[len(fence):]
                break
        if new_code.endswith("```"):
            new_code = new_code[:-3]
        new_code = new_code.strip()

        os.makedirs("./debug_workspace", exist_ok=True)
        sandbox = os.path.join("./debug_workspace", os.path.basename(filepath))
        with open(sandbox, "w", encoding="utf-8") as f:
            f.write(new_code + "\n")
        print(f"  📁 Песочница: {sandbox}")

        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(new_code + "\n")
        print(f"  ✓ Обновлён: {filepath}")

    return len(matches) > 0


def run_debug_mode(args):
    """Режим 1: дебаггинг тест-файла."""
    print(f"📡 Модель:   {args.model.upper()}")
    print(f"🧪 Тесты:    {args.test_file}")
    print(f"🔁 Попытки:  {args.attempts}")

    if not os.path.exists(args.test_file):
        print(f"\n❌ Файл не найден: {args.test_file}")
        sys.exit(1)

    ask_model       = import_model(args.model)
    attempt_history = []

    for attempt in range(args.attempts):
        print(f"\n{'='*60}")
        print(f"ПОПЫТКА {attempt + 1}/{args.attempts}  [{args.model.upper()}]")
        print(f"{'='*60}")

        print("[СИНТАКСИС] Проверка .py файлов...")
        check_syntax()

        code, test_output = run_tests(args.test_file, verbose=args.verbose)

        if code == 0:
            det_ok, det_report = check_determinism(args.test_file)
            if det_ok:
                print("\n" + "✅" * 20)
                print("ВСЕ ТЕСТЫ ПРОШЛИ. ДЕТЕРМИНИЗМ ПОДТВЕРЖДЁН.")
                print("✅" * 20)
                return True
            test_output += f"\n\n{det_report}"

        print(f"\n❌ Тесты упали. Анализирую...")
        context = collect_context(args.test_file)

        history_block = ""
        if attempt_history:
            history_block = "\n\n=== ИСТОРИЯ ПОПЫТОК (НЕ ПОВТОРЯТЬ) ===\n"
            for i, h in enumerate(attempt_history, 1):
                history_block += f"\n--- Попытка {i} ---\n{h}\n"

        prompt = (
            "Ты дебаггер TARE. Найди причину и исправь.\n"
            + TARE_RULES
            + history_block
            + f"\n\n=== ВЫВОД ТЕСТОВ ===\n{test_output}"
            + f"\n\n{context}"
        )

        print(f"\n[→] Отправка в {args.model.upper()}...")
        response = call_with_retry(ask_model, prompt, max_retries=2)  # <-- добавлен retry

        if not response:
            print("❌ Пустой ответ.")
            attempt_history.append(f"[{attempt+1}] Модель не ответила.")
            continue

        with open("model_response.log", "w", encoding="utf-8") as f:
            f.write(response)
        print(f"  📁 Ответ сохранён ({len(response)} символов)")
        attempt_history.append(f"[{attempt+1}] {response[:600]}")

        if apply_patch(response):
            syntax_ok, _ = check_syntax()
            if not syntax_ok:
                print("  ✗ Патч сломал синтаксис.")
                continue

        time.sleep(0.5)

    print(f"\n❌ Не удалось исправить за {args.attempts} попыток.")
    return False


# ==========================
# ═══════════════════════════
# РЕЖИМ 2: SCAFFOLD
# ═══════════════════════════
# ==========================

def run_scaffold_mode(args):
    """Режим 2: генерация модулей по манифесту."""
    print(f"📡 Модель:    {args.model.upper()}")
    print(f"📋 Манифест:  {args.manifest}")
    if args.phase:
        print(f"🎯 Фаза:      {args.phase}")

    # Загружаем манифест
    manifest = load_manifest(args.manifest)
    modules  = manifest.get("module", [])

    if not modules:
        print("❌ В манифесте нет модулей.")
        sys.exit(1)

    # Фильтр по фазе
    if args.phase:
        modules = [m for m in modules if m.get("phase") == args.phase]
        print(f"  Модулей в фазе {args.phase}: {len(modules)}")

    # Находим что нужно сгенерировать
    to_generate = []
    for module in modules:
        status = module.get("status", "missing")
        path   = module.get("path", "")

        # Пересчитываем реальный статус независимо от того что в манифесте
        if not os.path.exists(path):
            # Файла нет совсем
            status = "missing"
        else:
            # Файл есть — проверяем содержимое
            try:
                with open(path, "r", encoding="utf-8") as f:
                    src = f.read()
                # Заглушка = содержит NotImplementedError и мало кода
                is_stub = ("NotImplementedError" in src and len(src) < 800)
            except Exception:
                is_stub = True

            if is_stub:
                status = "stub"
            elif status == "done":
                # Файл не заглушка — проверяем тесты
                test_path = module.get("test", "")
                if test_path and os.path.exists(test_path):
                    code, _ = run_tests(test_path)
                    if code != 0:
                        status = "stub"
                else:
                    # Нет тест-файла — не можем подтвердить done
                    status = "stub"

        if status in ("missing", "stub"):
            to_generate.append(module)

    if not to_generate:
        print("\n✅ Все модули уже готовы (status=done).")
        return

    # Топологическая сортировка
    ordered = topological_sort(to_generate)

    # Показываем план
    print(f"\n📋 ПЛАН ГЕНЕРАЦИИ ({len(ordered)} модулей):")
    print(f"{'─'*60}")
    for i, m in enumerate(ordered, 1):
        deps = m.get("depends_on", [])
        dep_str = f" ← {', '.join(deps)}" if deps else ""
        print(f"  {i:2}. [{m.get('phase','?')}] {m['name']:30} "
              f"({m.get('status','?')}){dep_str}")
    print(f"{'─'*60}")

    if args.dry_run:
        print("\n[dry-run] Генерация не запускалась. Убери --dry-run.")
        return

    # Подтверждение
    try:
        answer = input(f"\nГенерировать {len(ordered)} модулей? [y/N] ").strip().lower()
    except EOFError:
        answer = "y"  # неинтерактивный режим

    if answer != "y":
        print("Отменено.")
        return

    ask_model = import_model(args.model)
    generated = 0
    failed    = 0

    for i, module in enumerate(ordered, 1):
        name = module["name"]
        path = module.get("path", "")
        test = module.get("test", "")

        print(f"\n{'='*60}")
        print(f"[{i}/{len(ordered)}] Генерация: {name}")
        print(f"  Файл:  {path}")
        print(f"  Тест:  {test}")
        print(f"{'='*60}")

        # Контекст — зависимости (их код)
        context_lines = ["\n=== КОНТЕКСТ (зависимости) ==="]
        for dep_name in module.get("depends_on", []):
            dep_module = next(
                (m for m in manifest.get("module", []) if m["name"] == dep_name),
                None
            )
            if dep_module:
                dep_path = dep_module.get("path", "")
                if os.path.exists(dep_path):
                    with open(dep_path, "r", encoding="utf-8") as f:
                        context_lines.append(
                            f"\n===== FILE: {dep_path} =====\n{f.read()}"
                        )

        context = "\n".join(context_lines)
        prompt  = build_scaffold_prompt(module, context)

        print(f"[→] Отправка в {args.model.upper()}...")
        response = call_with_retry(ask_model, prompt, max_retries=3)  # <-- добавлен retry

        if not response:
            print(f"❌ Пустой ответ для {name}.")
            failed += 1
            continue

        # Сохраняем ответ
        log_path = f"scaffold_{name}_response.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(response)
        print(f"  📁 Ответ → {log_path}")

        # Применяем патч
        if not apply_patch(response):
            print(f"  ⚠️ Патч не найден для {name}.")
            failed += 1
            continue

        # Проверяем синтаксис
        if os.path.exists(path):
            result = subprocess.run(
                [sys.executable, "-m", "py_compile", path],
                capture_output=True, text=True
            )
            if result.returncode != 0:
                print(f"  ✗ Синтаксическая ошибка: {result.stderr.strip()}")
                update_manifest_status(args.manifest, name, "stub")
                failed += 1
                continue
            print(f"  ✓ Синтаксис OK")

        # Запускаем тесты
        if test and os.path.exists(test):
            print(f"  [ТЕСТЫ] Запускаю {test}...")
            code, test_output = run_tests(test)
            if code == 0:
                print(f"  ✅ Тесты прошли!")
                update_manifest_status(args.manifest, name, "done")
                generated += 1
            else:
                print(f"  ⚠️ Тесты упали. Запускаю дебаггинг...")
                # Быстрый дебаггинг (2 попытки)
                success = _quick_debug(ask_model, test, test_output, args.attempts)
                if success:
                    update_manifest_status(args.manifest, name, "done")
                    generated += 1
                else:
                    update_manifest_status(args.manifest, name, "stub")
                    failed += 1
        else:
            # Нет тест-файла — считаем как stub
            update_manifest_status(args.manifest, name, "stub")
            generated += 1
            print(f"  ✓ Файл создан (тест-файл отсутствует)")

    print(f"\n{'='*60}")
    print(f"SCAFFOLD ЗАВЕРШЁН")
    print(f"  ✅ Сгенерировано: {generated}")
    print(f"  ❌ Ошибок:        {failed}")
    print(f"{'='*60}")


def _quick_debug(ask_model, test_path: str,
                 test_output: str, max_attempts: int = 2) -> bool:
    """Быстрый дебаггинг после scaffold — максимум 2 попытки."""
    for attempt in range(min(max_attempts, 2)):
        context = collect_context(test_path)
        prompt  = (
            "Ты дебаггер TARE. Только что сгенерированный модуль не прошёл тесты.\n"
            + TARE_RULES
            + f"\n\n=== ВЫВОД ТЕСТОВ ===\n{test_output}"
            + f"\n\n{context}"
        )
        response = call_with_retry(ask_model, prompt, max_retries=2)  # <-- добавлен retry
        if response and apply_patch(response):
            code, test_output = run_tests(test_path)
            if code == 0:
                return True
    return False


# ==========================
# ТОЧКА ВХОДА
# ==========================

def main():
    args = parse_args()

    print("=" * 60)
    print("TARE АВТОДЕБАГЕР v2")
    print("=" * 60)

    if args.scaffold:
        run_scaffold_mode(args)
    elif args.test_file:
        run_debug_mode(args)
    else:
        print("❌ Укажи тест-файл или флаг --scaffold")
        print("\nПримеры:")
        print("  python auto_debug_tare.py tare/time_engine/test_time_engine.py")
        print("  python auto_debug_tare.py --scaffold")
        print("  python auto_debug_tare.py --scaffold --phase 1 --dry-run")
        sys.exit(1)


if __name__ == "__main__":
    main()

 