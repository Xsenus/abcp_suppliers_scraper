# config.py

BASE_URL = "https://www.abcp.ru"
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}
MAX_WORKERS = 3            # Кол-во потоков
RETRY_ATTEMPTS = 5         # Сколько раз пробовать запрос
RETRY_DELAY = 5            # Задержка между попытками (в секундах)
CSV_FILENAME = "abcp_suppliers_full.csv"
JSON_FILENAME = "abcp_suppliers_full.json"
