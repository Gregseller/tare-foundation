# ai_ask_DeepSeek.py
import os
import sys
import logging
import datetime
import argparse
import json
import requests
from typing import Optional, Dict, Any
from dotenv import load_dotenv

AGENT_NAME = "deepseek"
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
RESPONSE_FILE = os.path.join(CURRENT_DIR, "response_prompt_4marvels.txt")
USAGE_LOG = os.path.join(CURRENT_DIR, "deepseek_usage.json")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("DEEPSEEK_API_KEY")

DEEPSEEK_PRICE_INPUT = 0.14
DEEPSEEK_PRICE_OUTPUT = 0.28

def load_usage() -> dict:
    if os.path.exists(USAGE_LOG):
        try:
            with open(USAGE_LOG, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "total_cost_usd": 0.0,
        "requests": []
    }

def save_usage(data: dict):
    data["last_updated"] = datetime.datetime.now().isoformat()
    with open(USAGE_LOG, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def calculate_cost(input_tokens: int, output_tokens: int) -> float:
    cost = (input_tokens / 1_000_000) * DEEPSEEK_PRICE_INPUT + \
           (output_tokens / 1_000_000) * DEEPSEEK_PRICE_OUTPUT
    return round(cost, 8)

def record_request(input_tokens: int, output_tokens: int, success: bool = True, error: str = None):
    cost = calculate_cost(input_tokens, output_tokens)
    usage = load_usage()
    usage["total_input_tokens"] += input_tokens
    usage["total_output_tokens"] += output_tokens
    usage["total_cost_usd"] += cost
    usage["requests"].append({
        "timestamp": datetime.datetime.now().isoformat(),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost,
        "success": success,
        "error": error
    })
    if len(usage["requests"]) > 100:
        usage["requests"] = usage["requests"][-100:]
    save_usage(usage)
    logger.info(f"💰 DeepSeek: ${cost:.6f} | in:{input_tokens}, out:{output_tokens}")

def ask_model(prompt: str, model: str = "deepseek-chat") -> Optional[str]:
    if not API_KEY:
        print("❌ DEEPSEEK_API_KEY не найден в .env")
        return None
    url = "https://api.deepseek.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 8192
    }
    try:
        response = requests.post(url, headers=headers, json=data, timeout=120)
        response.raise_for_status()
        result = response.json()
        if 'choices' in result and len(result['choices']) > 0:
            response_text = result['choices'][0]['message']['content']
            input_tokens = result.get('usage', {}).get('prompt_tokens', 0)
            output_tokens = result.get('usage', {}).get('completion_tokens', 0)
            if input_tokens == 0:
                input_tokens = len(prompt) // 4
            if output_tokens == 0:
                output_tokens = len(response_text) // 4
            record_request(input_tokens, output_tokens, success=True)
            return response_text
        else:
            logger.error(f"Неожиданный формат ответа: {result}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"DeepSeek API Error: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response body: {e.response.text}")
        record_request(0, 0, success=False, error=str(e))
        return None
    except Exception as e:
        logger.error(f"DeepSeek Error: {e}")
        record_request(0, 0, success=False, error=str(e))
        return None

def main():
    if not API_KEY:
        print("❌ DEEPSEEK_API_KEY не найден в .env")
        sys.exit(1)
    parser = argparse.ArgumentParser()
    parser.add_argument('--status', '-s', action='store_true', help='Показать статус')
    args, unknown = parser.parse_known_args()
    if args.status:
        usage = load_usage()
        print(f"💰 Общая стоимость: ${usage['total_cost_usd']:.6f}")
        print(f"📨 Запросов: {len(usage['requests'])}")
        return
    args_input = " ".join(unknown) if unknown else ""
    if not args_input:
        print("Использование: python ai_ask_DeepSeek.py \"Ваш запрос\"")
        sys.exit(0)
    result = ask_model(args_input)
    if result:
        print(result)
    else:
        print("Ошибка получения ответа.")

if __name__ == "__main__":
    main()


# # ============================================================
# # python  ai_ask_DeepSeek.py - Взаимодействие с DeepSeek API и учет расходов
# # python auto_debug_interactive.py main.py --model deepseek
# # ============================================================
# import os
# import sys
# import logging
# import datetime
# import argparse
# import json
# import requests
# from typing import Optional, Dict, Any
# from dotenv import load_dotenv

# # ============================================================
# # КОНФИГУРАЦИЯ
# # ============================================================

# AGENT_NAME = "deepseek"
# CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# RESPONSE_FILE = os.path.join(CURRENT_DIR, "response_prompt_4marvels.txt")
# USAGE_LOG = os.path.join(CURRENT_DIR, "deepseek_usage.json")

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# load_dotenv()
# API_KEY = os.getenv("DEEPSEEK_API_KEY")

# # Цены DeepSeek
# DEEPSEEK_PRICE_INPUT = 0.14   # $ за 1M токенов
# DEEPSEEK_PRICE_OUTPUT = 0.28  # $ за 1M токенов


# # ============================================================
# # УЧЕТ РАСХОДОВ
# # ============================================================

# def load_usage() -> dict:
#     if os.path.exists(USAGE_LOG):
#         try:
#             with open(USAGE_LOG, 'r', encoding='utf-8') as f:
#                 return json.load(f)
#         except:
#             pass
#     return {
#         "total_input_tokens": 0,
#         "total_output_tokens": 0,
#         "total_cost_usd": 0.0,
#         "requests": []
#     }


# def save_usage(data: dict):
#     data["last_updated"] = datetime.datetime.now().isoformat()
#     with open(USAGE_LOG, 'w', encoding='utf-8') as f:
#         json.dump(data, f, indent=2, ensure_ascii=False)


# def calculate_cost(input_tokens: int, output_tokens: int) -> float:
#     cost = (input_tokens / 1_000_000) * DEEPSEEK_PRICE_INPUT + \
#            (output_tokens / 1_000_000) * DEEPSEEK_PRICE_OUTPUT
#     return round(cost, 8)


# def record_request(input_tokens: int, output_tokens: int, success: bool = True, error: str = None):
#     cost = calculate_cost(input_tokens, output_tokens)
    
#     usage = load_usage()
#     usage["total_input_tokens"] += input_tokens
#     usage["total_output_tokens"] += output_tokens
#     usage["total_cost_usd"] += cost
#     usage["requests"].append({
#         "timestamp": datetime.datetime.now().isoformat(),
#         "input_tokens": input_tokens,
#         "output_tokens": output_tokens,
#         "cost_usd": cost,
#         "success": success,
#         "error": error
#     })
    
#     # Оставляем последние 100 запросов
#     if len(usage["requests"]) > 100:
#         usage["requests"] = usage["requests"][-100:]
    
#     save_usage(usage)
    
#     logger.info(f"💰 DeepSeek: ${cost:.6f} | in:{input_tokens}, out:{output_tokens}")
#     logger.info(f"📊 Всего: ${usage['total_cost_usd']:.6f} | запросов: {len(usage['requests'])}")


# # ============================================================
# # DEEPSEEK CLIENT (прямой через requests)
# # ============================================================

# def ask_model(prompt: str, model: str = "deepseek-chat") -> Optional[str]:
#     """Основной интерфейс для вызова DeepSeek через requests"""
    
#     if not API_KEY:
#         print("❌ DEEPSEEK_API_KEY не найден в .env")
#         return None
    
#     url = "https://api.deepseek.com/v1/chat/completions"
#     headers = {
#         "Authorization": f"Bearer {API_KEY}",
#         "Content-Type": "application/json"
#     }
#     data = {
#         "model": model,
#         "messages": [{"role": "user", "content": prompt}],
#         "temperature": 0.3,
#         "max_tokens": 8192
#     }
    
#     try:
#         response = requests.post(url, headers=headers, json=data, timeout=120)
#         response.raise_for_status()
        
#         result = response.json()
        
#         if 'choices' in result and len(result['choices']) > 0:
#             response_text = result['choices'][0]['message']['content']
            
#             # Получаем количество токенов из ответа
#             input_tokens = result.get('usage', {}).get('prompt_tokens', 0)
#             output_tokens = result.get('usage', {}).get('completion_tokens', 0)
            
#             # Если токены не вернулись, считаем приблизительно
#             if input_tokens == 0:
#                 input_tokens = len(prompt) // 4
#             if output_tokens == 0:
#                 output_tokens = len(response_text) // 4
            
#             record_request(input_tokens, output_tokens, success=True)
            
#             return response_text
#         else:
#             logger.error(f"Неожиданный формат ответа: {result}")
#             return None
            
#     except requests.exceptions.RequestException as e:
#         logger.error(f"DeepSeek API Error: {e}")
#         if hasattr(e, 'response') and e.response:
#             logger.error(f"Response body: {e.response.text}")
#         record_request(0, 0, success=False, error=str(e))
#         return None
#     except Exception as e:
#         logger.error(f"DeepSeek Error: {e}")
#         record_request(0, 0, success=False, error=str(e))
#         return None


# def get_status() -> Dict[str, Any]:
#     usage = load_usage()
#     return {
#         "total_cost_usd": round(usage["total_cost_usd"], 6),
#         "total_requests": len(usage["requests"]),
#         "total_input_tokens": usage["total_input_tokens"],
#         "total_output_tokens": usage["total_output_tokens"],
#     }


# # ============================================================
# # MAIN
# # ============================================================

# def main():
#     if not API_KEY:
#         print("❌ DEEPSEEK_API_KEY не найден в .env")
#         sys.exit(1)

#     parser = argparse.ArgumentParser()
#     parser.add_argument('--status', '-s', action='store_true', help='Показать статус')
#     args, unknown = parser.parse_known_args()

#     if args.status:
#         status = get_status()
#         print("\n📊 DEEPSEEK API - СТАТУС")
#         print(f"💸 Общая стоимость: ${status['total_cost_usd']:.6f}")
#         print(f"📨 Всего запросов: {status['total_requests']}")
#         print(f"📝 Входящие токены: {status['total_input_tokens']:,}")
#         print(f"📝 Исходящие токены: {status['total_output_tokens']:,}")
#         return

#     args_input = " ".join(unknown) if unknown else ""
    
#     if not args_input:
#         print("Использование: python ai_ask_DeepSeek.py \"Ваш запрос\"")
#         print("          python ai_ask_DeepSeek.py --status")
#         sys.exit(0)
    
#     result = ask_model(args_input)
#     if result:
#         print(result)
#     else:
#         print("Ошибка получения ответа.")


# if __name__ == "__main__":
#     main()