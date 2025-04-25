# ABCP Supplier Scraper

Скрипт для сбора информации о поставщиках с сайта [ABCP.ru](https://www.abcp.ru).

## Особенности
- Сбор данных по всем странам
- Парсинг контактных данных с профиля
- Автоматический retry при ошибках
- Многопоточность для ускорения
- Прогрессбар и логирование
- Сохранение промежуточных результатов

## Установка

```bash
pip install -r requirements.txt
```

## Запуск

```bash
python abcp_scraper.py
```

## Выходные файлы

- `abcp_suppliers_full.csv` — полный результат (разделённые телефоны/email)
- `abcp_suppliers_partial.csv` — сохраняется после каждой страны
- `abcp_suppliers_full.json` — полные данные
- `abcp_scraper.log` — лог-файл
