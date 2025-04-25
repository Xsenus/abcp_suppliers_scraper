# abcp_scraper_threaded_commented.py

# Импорты стандартных библиотек
import requests                               # Для выполнения HTTP-запросов
from bs4 import BeautifulSoup                 # Для парсинга HTML
import pandas as pd                           # Для записи CSV
import json                                   # Для сохранения JSON
import time                                   # Для паузы между попытками
import re                                     # Для нормализации телефонов
import logging                                # Для логирования процесса
from concurrent.futures import ThreadPoolExecutor, as_completed  # Многопоточность
from requests.adapters import HTTPAdapter     # Для конфигурации повторных попыток
from urllib3.util.retry import Retry          # Логика повторных запросов
from config import BASE_URL, HEADERS, MAX_WORKERS, RETRY_ATTEMPTS, RETRY_DELAY, CSV_FILENAME, JSON_FILENAME

# --- Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- Настройка HTTP-сессии с автоматическим повтором запросов
session = requests.Session()
retries = Retry(total=RETRY_ATTEMPTS, backoff_factor=RETRY_DELAY, status_forcelist=[502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("http://", adapter)
session.mount("https://", adapter)

# --- Функция нормализации телефона до формата 7XXXXXXXXXX
def normalize_phone(phone_raw: str) -> str:
    phone = re.sub(r"[^\d]", "", phone_raw)  # Удаляем все кроме цифр
    if phone.startswith("8") and len(phone) == 11:
        phone = "7" + phone[1:]
    elif phone.startswith("9") and len(phone) == 10:
        phone = "7" + phone
    return phone if len(phone) == 11 else ""

# --- Получение всех стран-поставщиков
def get_country_links():
    response = session.get(f"{BASE_URL}/suppliers", headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    country_links = []
    for group in soup.select("div.suppliers-btn-group"):
        for a in group.find_all("a", href=True):
            name = a.text.strip()
            href = a["href"]
            full_url = href if href.startswith("http") else BASE_URL + href
            country_links.append({"name": name, "url": full_url})
    return country_links

# --- Получение контактной информации поставщика
def get_supplier_contact_info(profile_url):
    contact_info = {
        "Контактный сайт": "",
        "Телефоны": [],
        "Email’ы": []
    }

    # Пробуем несколько раз (если сервер не отвечает)
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            response = session.get(profile_url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            block = soup.find("div", class_="fr-panel-body")
            if not block:
                raise ValueError("Контейнер с контактами не найден")

            # Сайт
            site_tag = block.select_one("a[href^='http']")
            if site_tag:
                contact_info["Контактный сайт"] = site_tag.get_text(strip=True)

            phones = set()
            emails = set()

            # Email'ы
            email_tags = block.select("a[href^='mailto']")
            for tag in email_tags:
                text = tag.text.strip()
                if "@" in text:
                    emails.add(text.lower())

            # Телефоны — из ссылок <a href="tel:...">
            tel_tags = block.select("a[href^='tel']")
            for tag in tel_tags:
                raw = tag.text.strip()
                phone_main = re.split(r"(доб\.?\s*\d+)", raw, maxsplit=1, flags=re.IGNORECASE)[0].strip()
                normalized = normalize_phone(phone_main)
                if normalized:
                    phones.add(normalized)

            # Телефоны — просто текстом
            for line in block.stripped_strings:
                if re.search(r"(телефон|mobile|phone|mob\.?):?\s*[\+\d\s\-\(\)]{6,}", line, flags=re.IGNORECASE):
                    matches = re.findall(r"(\+?\d[\d\-\s\(\)]{6,})", line)
                    for match in matches:
                        normalized = normalize_phone(match)
                        if normalized:
                            phones.add(normalized)

            contact_info["Телефоны"] = sorted(phones)
            contact_info["Email’ы"] = sorted(emails)
            return contact_info

        except Exception as e:
            logging.warning(f"Попытка {attempt} не удалась: {e}")
            time.sleep(RETRY_DELAY)

    logging.error(f"❌ Не удалось получить профиль: {profile_url}")
    return contact_info

# --- Парсинг одной строки (одного поставщика)
def parse_supplier(row, country_name):
    try:
        cols = row.find_all("td")
        if len(cols) < 8:
            return None

        number = cols[0].text.strip()
        rating_abcp = cols[1].text.strip()
        links = cols[2].select("a[href*='/suppliers']")
        name = links[1].text.strip() if len(links) > 1 else ""
        profile_url = links[1]["href"] if len(links) > 1 else ""
        if not profile_url.startswith("http"):
            profile_url = BASE_URL + profile_url
        site_span = cols[4].find("span")
        website = site_span.text.strip() if site_span else ""
        rating_span = row.select_one("span.starsBlock")
        rating = rating_span["title"].replace("Средний рейтинг", "").replace("&nbsp;", "").strip() if rating_span and rating_span.has_attr("title") else ""
        reviews_span = row.select_one("span.reviewsQuant")
        reviews = reviews_span.text.strip() if reviews_span else ""
        availability = cols[6].text.strip()
        response_time = cols[7].text.strip()

        logging.info(f"👷‍♀️ Поток: получаем {name}")
        contact_info = get_supplier_contact_info(profile_url)

        return {
            "ID": None,
            "Страна": country_name,
            "Номер в таблице": number,
            "Рейтинг ABCP": rating_abcp,
            "Название": name,
            "Ссылка": profile_url,
            "Сайт": website,
            "Рейтинг (число)": rating,
            "Отзывы": reviews,
            "Доступность": availability,
            "Время ответа (сек)": response_time,
            **contact_info
        }
    except Exception as e:
        logging.error(f"❌ Ошибка при парсинге строки: {e}")
        return None

# --- Парсинг всех поставщиков в стране
def parse_suppliers_from_country(country_name, url):
    response = session.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
    rows = soup.select("tr.distributor_row")

    suppliers = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_row = {executor.submit(parse_supplier, row, country_name): row for row in rows}
        for future in as_completed(future_to_row):
            supplier = future.result()
            if supplier:
                suppliers.append(supplier)

    return suppliers

# --- Основная логика скрипта
def main():
    all_suppliers = []
    countries = get_country_links()

    for country in countries:
        logging.info(f"🌍 Парсим страну: {country['name']}")
        try:
            suppliers = parse_suppliers_from_country(country["name"], country["url"])
            all_suppliers.extend(suppliers)
        except Exception as e:
            logging.error(f"❌ Ошибка при обработке страны {country['name']}: {e}")

    # Присваиваем ID
    for i, s in enumerate(all_suppliers, 1):
        s["ID"] = i

    # Преобразуем телефоны и email в плоский вид (ТОЛЬКО для CSV)
    for supplier in all_suppliers:
        phones = supplier.get("Телефоны", [])
        emails = supplier.get("Email’ы", [])
        for idx, phone in enumerate(phones, 1):
            supplier[f"Телефон {idx}"] = phone
        for idx, email in enumerate(emails, 1):
            supplier[f"Email {idx}"] = email
        supplier.pop("Телефоны", None)
        supplier.pop("Email’ы", None)

    # Сохраняем CSV
    df = pd.DataFrame(all_suppliers)
    df.to_csv(CSV_FILENAME, sep=";", index=False, encoding="utf-8-sig")

    # Сохраняем JSON (оставляем списки телефонов и email'ов)
    for supplier in all_suppliers:
        supplier["Телефоны"] = supplier.pop("Телефон 1", None)
        supplier["Email’ы"] = supplier.pop("Email 1", None)

    with open(JSON_FILENAME, "w", encoding="utf-8") as f:
        json.dump(all_suppliers, f, ensure_ascii=False, indent=4)

    logging.info(f"✅ Готово! Сохранено в {CSV_FILENAME} и {JSON_FILENAME}")

# --- Точка входа
if __name__ == "__main__":
    main()