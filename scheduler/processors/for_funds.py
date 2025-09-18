# app/processors/for_funds.py

import time

# Маппинг полей из marketdata → наша модель
FUND_MARKETDATA_MAP = {
    "SECID": "secid",
    "BOARDID": "boardid",
    "LAST": "last_price",
    "OPEN": "open_price",
    "HIGH": "high_price",
    "LOW": "low_price",
    "VALTODAY": "volume",              # Объём торгов в деньгах
    "NUMTRADES": "trades_count",       # Количество сделок
    # Убраны: capitalization, change_capitalization, trading_status
}

# Поля из securities
SEC_FIELDS_MAP = {
    "SHORTNAME": "shortname",
    "PREVPRICE": "prev_price",
    "FACEUNIT": "currency",
    "LISTLEVEL": "list_level",         # ← добавлено
}


def process_fund_data(raw_data):
    """
    Обрабатывает данные по фондам (ETF) с API Мосбиржи и приводит к общей модели.
    :param raw_data: dict — ответ от /iss/engines/stock/markets/seltra/boards/TQTF/securities.json
    :return: list[dict] — готово к вставке в market_data
    """
    start = time.time()

    # Извлекаем marketdata
    market = raw_data.get("marketdata")
    if not market or "data" not in market:
        print("WARNING: 'marketdata' missing or invalid")
        return []

    m_columns = market["columns"]
    m_rows = market["data"]
    m_col_idx = {col: idx for idx, col in enumerate(m_columns)}

    # Извлекаем securities
    securities = raw_data.get("securities")
    if not securities or "data" not in securities:
        print("WARNING: 'securities' missing or invalid")
        return []

    s_columns = securities["columns"]
    s_rows = securities["data"]
    s_col_idx = {col: idx for idx, col in enumerate(s_columns)}

    # Проверка SECID в securities
    secid_idx = s_col_idx.get("SECID")
    if secid_idx is None:
        print("ERROR: 'SECID' column not found in securities")
        return []

    # Собираем данные из securities
    secid_to_info = {}
    for row in s_rows:
        if len(row) != len(s_columns):
            continue

        secid = row[secid_idx]
        sec_info = {}

        for moex_field, local_field in SEC_FIELDS_MAP.items():
            if moex_field not in s_col_idx:
                continue
            value = row[s_col_idx[moex_field]]
            if value == "" or value is None:
                value = None

            # Приведение LISTLEVEL к int
            if moex_field == "LISTLEVEL":
                try:
                    value = int(value) if value not in (None, "") else None
                except (ValueError, TypeError):
                    value = None

            sec_info[local_field] = value

        # Убедимся, что shortname есть
        sec_info["shortname"] = sec_info.get("shortname") or row[s_col_idx.get("SHORTNAME")] or f"Fund {secid}"

        secid_to_info[secid] = sec_info

    # Проверяем обязательные поля в marketdata
    required_market_cols = ["SECID", "BOARDID"]
    if not all(col in m_col_idx for col in required_market_cols):
        print("ERROR: Required columns missing in marketdata")
        return []

    parsed = []

    for row in m_rows:
        if len(row) != len(m_columns):
            continue

        secid = row[m_col_idx["SECID"]]
        boardid = row[m_col_idx["BOARDID"]]

        item = {
            "secid": secid,
            "boardid": boardid,
            "instrument_type": "fund",
        }

        # Заполняем поля из marketdata
        for moex_field, local_field in FUND_MARKETDATA_MAP.items():
            if moex_field not in m_col_idx:
                continue
            value = row[m_col_idx[moex_field]]
            if value == "" or value is None:
                value = None

            # Конвертируем числовые поля
            if local_field in ("last_price", "open_price", "high_price", "low_price"):
                try:
                    value = float(value) if value is not None else None
                except (ValueError, TypeError):
                    value = None
            elif local_field == "volume":
                try:
                    value = int(float(value)) if value not in (None, "") else None
                except (ValueError, TypeError):
                    value = None
            elif local_field == "trades_count":
                try:
                    value = int(value) if value not in (None, "") else None
                except (ValueError, TypeError):
                    value = None

            item[local_field] = value

        # Добавляем данные из securities
        sec_info = secid_to_info.get(secid, {})

        item["shortname"] = sec_info.get("shortname")
        item["currency"] = sec_info.get("currency", "SUR")  # SUR — рубли по умолчанию
        item["list_level"] = sec_info.get("list_level")
        prev_price = sec_info.get("prev_price")

        # Расчёт change_abs и change_percent
        last_price = item.get("last_price")
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

        # === Поля update_time, trading_status, capitalization — УБРАНЫ ===

        parsed.append(item)

    print(f"[Funds] Обработано {len(parsed)} фондов за {time.time() - start:.2f} сек")
    return parsed