import time
from datetime import datetime

# Маппинг полей из API → наша модель
FIELDS_MAP = {
    "SECID": "secid",
    "BOARDID": "boardid",
    "LAST": "last_price",
    "OPEN": "open_price",
    "HIGH": "high_price",
    "LOW": "low_price",
    "VALTODAY": "volume",                # Объём торгов (в деньгах)
    "NUMTRADES": "trades_count",         # Количество сделок
    "ISSUECAPITALIZATION": "capitalization",
    "TRENDISSUECAPITALIZATION": "change_capitalization",
    # Убрали: TRADINGSTATUS, VOLTODAY
}

# Поля из securities (для доп. данных)
SEC_FIELDS_MAP = {
    "SHORTNAME": "shortname",
    "PREVPRICE": "prev_price",
    "CURRENCYID": "currency",
    "LISTLEVEL": "list_level",           # ← новый ключ: уровень листинга
}


def process_stock_data(raw_data):
    start = time.time()

    # Извлекаем данные
    marketdata = raw_data["marketdata"]
    columns = marketdata["columns"]
    rows = marketdata["data"]
    col_idx = {col: idx for idx, col in enumerate(columns)}

    securities = raw_data["securities"]
    sec_columns = securities["columns"]
    sec_rows = securities["data"]
    sec_col_idx = {col: idx for idx, col in enumerate(sec_columns)}

    # Словарь для данных из securities по secid
    secid_to_data = {}
    secid_idx = sec_col_idx.get("SECID")
    if secid_idx is None:
        return []

    for row in sec_rows:
        secid = row[secid_idx]
        sec_data = {}
        for moex_field, local_field in SEC_FIELDS_MAP.items():
            if moex_field in sec_col_idx:
                value = row[sec_col_idx[moex_field]]
                # Приводим пустые строки к None
                if value == "" or value is None:
                    value = None
                # Явно конвертируем LISTLEVEL в int, если возможно
                if moex_field == "LISTLEVEL":
                    value = int(value) if value not in (None, "") else None
                sec_data[local_field] = value
        secid_to_data[secid] = sec_data

    # Результат
    parsed = []

    # Проверяем обязательные поля
    required_cols = ["SECID", "BOARDID"]
    if not all(col in col_idx for col in required_cols):
        return []

    for row in rows:
        secid = row[col_idx["SECID"]]
        boardid = row[col_idx["BOARDID"]]

        item = {
            "secid": secid,
            "boardid": boardid,
            "instrument_type": "stock",
        }

        # Заполняем поля из marketdata
        for moex_field, local_field in FIELDS_MAP.items():
            if moex_field not in col_idx:
                continue
            value = row[col_idx[moex_field]]
            if value == "" or value is None:
                value = None
            item[local_field] = value

        # Добавляем данные из securities
        sec_data = secid_to_data.get(secid, {})
        item["shortname"] = sec_data.get("shortname")
        item["currency"] = sec_data.get("currency")
        item["list_level"] = sec_data.get("list_level")  # ← добавлено

        # Расчёт change_abs и change_percent через PREVPRICE
        last_price = item.get("last_price")
        prev_price = sec_data.get("prev_price")
        if last_price is not None and prev_price is not None and prev_price != 0:
            item["change_abs"] = round(last_price - prev_price, 8)
            item["change_percent"] = round((last_price - prev_price) / prev_price * 100, 6)
        else:
            item["change_abs"] = None
            item["change_percent"] = None

        # Волатильность: (high - low) / open * 100
        open_price = item.get("open_price")
        high_price = item.get("high_price")
        low_price = item.get("low_price")
        if all(v is not None for v in [high_price, low_price, open_price]) and open_price != 0:
            item["volatility_percent"] = round((high_price - low_price) / open_price * 100, 6)
        else:
            item["volatility_percent"] = None

        # === Убрали update_time и trading_status ===
        # Больше нигде не используется

        parsed.append(item)

    print(f"[Stocks] Обработано {len(parsed)} инструментов за {time.time() - start:.2f} сек")
    return parsed