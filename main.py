import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import re

BASE_URL = "https://www.abcp.ru"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def normalize_phone(phone_raw: str) -> str:
    phone = re.sub(r"[^\d]", "", phone_raw)
    if phone.startswith("8") and len(phone) == 11:
        phone = "7" + phone[1:]
    elif phone.startswith("9") and len(phone) == 10:
        phone = "7" + phone
    return phone if len(phone) == 11 else ""

def get_country_links():
    response = requests.get(f"{BASE_URL}/suppliers", headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    country_links = []

    for group in soup.select("div.suppliers-btn-group"):
        for a in group.find_all("a", href=True):
            name = a.text.strip()
            href = a["href"]
            full_url = href if href.startswith("http") else BASE_URL + href
            country_links.append({"name": name, "url": full_url})

    return country_links

import re
from bs4 import BeautifulSoup

def normalize_phone(phone_raw: str) -> str:
    phone = re.sub(r"[^\d]", "", phone_raw)
    if phone.startswith("8") and len(phone) == 11:
        phone = "7" + phone[1:]
    elif phone.startswith("9") and len(phone) == 10:
        phone = "7" + phone
    return phone if len(phone) == 11 else ""

def get_supplier_contact_info(profile_url):
    contact_info = {
        "Контактный сайт": "",
        "Телефоны": [],
        "Email’ы": []
    }

    try:
        response = requests.get(profile_url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        block = soup.find("div", class_="fr-panel-body")
        if not block:
            return contact_info

        # --- Контактный сайт
        site_tag = block.select_one("a[href^='http']")
        if site_tag:
            contact_info["Контактный сайт"] = site_tag.get_text(strip=True)

        phones = set()
        emails = set()

        # --- Email’ы
        email_tags = block.select("a[href^='mailto']")
        for tag in email_tags:
            text = tag.text.strip()
            if "@" in text:
                emails.add(text.lower())

        # --- Телефоны из <a href="tel:">
        tel_tags = block.select("a[href^='tel']")
        for tag in tel_tags:
            raw = tag.text.strip()
            phone_main = re.split(r"(доб\.?\s*\d+)", raw, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            normalized = normalize_phone(phone_main)
            if normalized:
                phones.add(normalized)

        # --- Телефоны из текста
        for line in block.stripped_strings:
            if re.search(r"(телефон|mobile|phone|mob\.?):?\s*[\+\d\s\-\(\)]{6,}", line, flags=re.IGNORECASE):
                matches = re.findall(r"(\+?\d[\d\-\s\(\)]{6,})", line)
                for match in matches:
                    normalized = normalize_phone(match)
                    if normalized:
                        phones.add(normalized)

        contact_info["Телефоны"] = sorted(phones)
        contact_info["Email’ы"] = sorted(emails)

    except Exception as e:
        print(f"❌ Ошибка при парсинге профиля: {profile_url}\n{e}")

    return contact_info

def parse_suppliers_from_country(country_name, url):
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
    rows = soup.select("tr.distributor_row")

    suppliers = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 8:
            continue

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

        supplier = {
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
            "Время ответа (сек)": response_time
        }

        # Контактная информация с профиля
        print(f"👷‍♀️ Получаем информацию о контакте: {name}")
        contact_info = get_supplier_contact_info(profile_url)
        supplier.update(contact_info)

        suppliers.append(supplier)
        time.sleep(0.5)

    return suppliers

# Основной запуск
all_suppliers = []
countries = get_country_links()

for country in countries:
    print(f"🌍 Парсим страну: {country['name']}")
    try:
        suppliers = parse_suppliers_from_country(country["name"], country["url"])
        all_suppliers.extend(suppliers)
    except Exception as e:
        print(f"❌ Ошибка при обработке страны {country['name']}: {e}")

# Добавляем ID
for i, s in enumerate(all_suppliers, 1):
    s["ID"] = i

# 👉 Расплющиваем списки для CSV
for supplier in all_suppliers:
    phones = supplier.get("Телефоны", [])
    emails = supplier.get("Email’ы", [])
    for idx, phone in enumerate(phones, 1):
        supplier[f"Телефон {idx}"] = phone
    for idx, email in enumerate(emails, 1):
        supplier[f"Email {idx}"] = email
    supplier.pop("Телефоны", None)
    supplier.pop("Email’ы", None)

# Сохраняем CSV + JSON
df = pd.DataFrame(all_suppliers)
df.to_csv("abcp_suppliers_full.csv", sep=";", index=False, encoding="utf-8-sig")

with open("abcp_suppliers_full.json", "w", encoding="utf-8") as f:
    json.dump(all_suppliers, f, ensure_ascii=False, indent=4)

print("✅ Готово! Сохранено в abcp_suppliers_full.csv и abcp_suppliers_full.json")
