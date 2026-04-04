# ai_ask_Claude.py
import os
import sys
import logging
import datetime
import argparse
import json
from dotenv import load_dotenv
import anthropic

# --- КОНФИГУРАЦИЯ ---
AGENT_NAME = "claude"
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
RESPONSE_FILE = os.path.join(CURRENT_DIR, "response_prompt_4marvels.txt")
COST_FILE = os.path.join(CURRENT_DIR, "api_costs.json")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("ANTHROPIC_API_KEY")

# --- ЦЕНЫ ($ за 1M токенов) ---
MODEL_PRICES = {
    "claude-sonnet-4-20250514":  {"input": 3.00,  "output": 15.00},
    "claude-haiku-4-5-20251001": {"input": 0.80,  "output": 4.00},
    "claude-opus-4-6":           {"input": 15.00, "output": 75.00},
}
MODEL = "claude-haiku-4-5-20251001"

# --- УЧЁТ РАСХОДОВ ---
def load_costs():
    if os.path.exists(COST_FILE):
        with open(COST_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"sessions": [], "total_usd": 0.0, "total_input_tokens": 0, "total_output_tokens": 0}

def save_costs(data):
    with open(COST_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def track_cost(input_tokens, output_tokens):
    prices = MODEL_PRICES.get(MODEL, {"input": 3.00, "output": 15.00})
    cost = (input_tokens * prices["input"] + output_tokens * prices["output"]) / 1_000_000

    data = load_costs()
    data["sessions"].append({
        "date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "model": MODEL,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": round(cost, 6)
    })
    data["total_usd"] = round(data["total_usd"] + cost, 6)
    data["total_input_tokens"] += input_tokens
    data["total_output_tokens"] += output_tokens
    save_costs(data)

    # Показываем в терминале
    print(f"\n💰 Этот запрос:  ${cost:.4f}  |  in: {input_tokens}  out: {output_tokens} токенов")
    print(f"📊 Всего потрачено: ${data['total_usd']:.4f}  (сессий: {len(data['sessions'])})")
    return cost

# --- ЧТЕНИЕ ФАЙЛОВ ---
def read_files_from_paths(paths):
    contents = []
    for path in paths:
        if os.path.isfile(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    contents.append(f"\n--- {os.path.basename(path)} ---\n{f.read()}\n")
                    logger.info(f"Файл загружен: {path}")
            except Exception as e:
                logger.error(f"Ошибка чтения файла {path}: {e}")
        elif os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for file in files:
                    if file.endswith(('.py', '.txt', '.json', '.md', '.yml', '.yaml', '.cfg', '.conf')):
                        file_path = os.path.join(root, file)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                rel_path = os.path.relpath(file_path, path)
                                contents.append(f"\n--- {rel_path} ---\n{f.read()}\n")
                        except Exception as e:
                            logger.error(f"Ошибка чтения {file_path}: {e}")
    return "\n".join(contents)

# --- СОХРАНЕНИЕ ОТВЕТА ---
def save_response(response_text):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"\n\n=== [{timestamp}] AGENT: {AGENT_NAME} ===\n"
    try:
        with open(RESPONSE_FILE, "a", encoding="utf-8") as f:
            f.write(header + response_text + "\n")
        logger.info(f"✅ Ответ сохранен в: {RESPONSE_FILE}")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка записи файла: {e}")
        return False


# --- ЗАПРОС К API ---
def ask_model(prompt, max_tokens=16000):
    try:
        client = anthropic.Anthropic(api_key=API_KEY)
        response = client.messages.create(
            model=MODEL,
            max_tokens=max_tokens,
            system="Ты технический эксперт. Отвечай точно и кратко.",
            messages=[{"role": "user", "content": prompt}]
        )

        # Автоматически считаем токены и стоимость
        track_cost(response.usage.input_tokens, response.usage.output_tokens)

        return response.content[0].text
    except Exception as e:
        logger.error(f"API Error: {e}")
        return None

# --- ГЛАВНАЯ ФУНКЦИЯ ---
def main():
    if not API_KEY:
        logger.error("ANTHROPIC_API_KEY не найден в .env")
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument('--files', '-f', nargs='+', help='Файлы для анализа')
    parser.add_argument('--folders', nargs='+', help='Папки для анализа (рекурсивно)')
                    
    args, unknown = parser.parse_known_args()

    args_input = " ".join(unknown) if unknown else ""
    stdin_content = ""
    if not sys.stdin.isatty():
        stdin_content = sys.stdin.read().strip()

    all_paths = []
    if args.files:
        all_paths.extend(args.files)
    if args.folders:
        all_paths.extend(args.folders)

    files_content = read_files_from_paths(all_paths) if all_paths else ""

    base_parts = []
    if stdin_content:
        base_parts.append(f"Контекст из stdin:\n{stdin_content}")
    if files_content:
        base_parts.append(f"Содержимое файлов:\n{files_content}")
    if args_input:
        base_parts.append(f"Запрос:\n{args_input}")

    if not base_parts:
        print("Использование: python ai_ask_Claude.py --files file1.py \"Вопрос\"")
        sys.exit(0)

    base_prompt = "\n\n".join(base_parts)
    current_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    final_prompt = (
        f"Системная информация: Текущая дата и время: {current_now}.\n"
        f"Инструкция: Начни ответ строго со строки: [Дата: {current_now}], "
        f"затем новая строка и ответ.\n\n"
        f"{base_prompt}"
    )

    logger.info(f"Запрос к {AGENT_NAME}...")
    result = ask_model(final_prompt)

    if result:
        print(result)
        save_response(result)
    else:
        print("Нет ответа от модели.")

if __name__ == "__main__":
    main()


