# scheduler/processors/for_indices.py

import time
import logging

from scheduler.clients.moex_client import MOEXClient
from scheduler.database.dao import upsert_market_data
from scheduler.database.engine import get_db

logger = logging.getLogger("scheduler.indices")

# Маппинг полей из marketdata → наша общая модель
INDEX_FIELDS_MAP = {
    "SECID": "secid",
    "BOARDID": "boardid",
    "CURRENTVALUE": "last_price",
    "OPENVALUE": "open_price",
    "HIGH": "high_price",
    "LOW": "low_price",
    "VALTODAY": "volume",
    "CAPITALIZATION": "capitalization",
    "LASTCHANGE": "change_abs",
    "LASTCHANGEPRC": "change_percent",
}

# Поля из securities
SEC_FIELDS_MAP = {
    "SHORTNAME": "shortname",
    "CURRENCYID": "currency",
    "ANNUALHIGH": "annual_high",
    "ANNUALLOW": "annual_low",
}


def process_index_data(raw_data):
    """
    Обрабатывает данные по индексам с API Мосбиржи.
    :param raw_data: dict — ответ от /iss/engines/stock/markets/index/...
    :return: list[dict] — готово к вставке в market_data
    """
    start = time.time()

    VALID_BOARDIDS = {"RTSI", "SNDX"}

    # === 1. Парсим securities ===
    securities_data = raw_data.get("securities")
    if not securities_data or "data" not in securities_data:
        logger.warning("'securities' missing or invalid")
        return []

    sec_columns = securities_data["columns"]
    sec_rows = securities_data["data"]
    sec_col_idx = {col: idx for idx, col in enumerate(sec_columns)}

    secid_to_info = {}
    valid_secids = set()

    for row in sec_rows:
        if len(row) != len(sec_columns):
            continue

        secid = row[sec_col_idx["SECID"]]
        boardid = row[sec_col_idx.get("BOARDID")]

        if boardid not in VALID_BOARDIDS:
            continue

        valid_secids.add(secid)

        sec_info = {}
        for moex_field, local_field in SEC_FIELDS_MAP.items():
            if moex_field not in sec_col_idx:
                continue
            value = row[sec_col_idx[moex_field]]
            if value == "" or value is None:
                value = None
            sec_info[local_field] = value

        shortname = sec_info.get("shortname")
        if isinstance(shortname, str) and "iNAV" in shortname:
            sec_info["shortname"] = shortname.replace("iNAV", "").strip()

        secid_to_info[secid] = sec_info

    # === 2. Парсим marketdata ===
    market = raw_data.get("marketdata")
    if not market or "data" not in market:
        logger.warning("'marketdata' missing or invalid")
        return []

    m_columns = market["columns"]
    m_rows = market["data"]
    m_col_idx = {col: idx for idx, col in enumerate(m_columns)}

    processed = []

    for row in m_rows:
        if len(row) != len(m_columns):
            continue

        secid = row[m_col_idx.get("SECID")]
        boardid = row[m_col_idx.get("BOARDID")]

        if not secid or not boardid:
            continue
        if boardid not in VALID_BOARDIDS or secid not in valid_secids:
            continue

        # === Проверка на VALTODAY (volume) ===
        valtoday_idx = m_col_idx.get("VALTODAY")
        valtoday = None
        if valtoday_idx is not None and valtoday_idx < len(row):
            valtoday = row[valtoday_idx]

        # Пропускаем, если объём отсутствует, null, пустая строка или 0
        if valtoday in (None, "", 0, "0"):
            logger.debug(f"Пропускаем {secid} (board {boardid}): VALTODAY = {valtoday} → индекс не торгуется")
            continue

        item = {
            "secid": secid,
            "boardid": boardid,
            "instrument_type": "index",
        }

        for moex_field, local_field in INDEX_FIELDS_MAP.items():
            if moex_field not in m_col_idx:
                continue
            value = row[m_col_idx[moex_field]]
            if value == "" or value is None:
                value = None

            if local_field in (
                "last_price", "open_price", "high_price", "low_price",
                "volume", "capitalization", "change_abs", "change_percent"
            ):
                try:
                    value = float(value) if value is not None else None
                except (ValueError, TypeError):
                    value = None

            item[local_field] = value

        sec_info = secid_to_info.get(secid, {})
        item["shortname"] = sec_info.get("shortname")
        item["currency"] = sec_info.get("currency")
        item["annual_high"] = sec_info.get("annual_high")
        item["annual_low"] = sec_info.get("annual_low")

        current_price = item.get("last_price")
        open_price = item.get("open_price")

        if item.get("change_abs") is None and current_price is not None and open_price is not None:
            item["change_abs"] = round(current_price - open_price, 8)

        if item.get("change_percent") is None and open_price and open_price != 0:
            item["change_percent"] = round((current_price - open_price) / open_price * 100, 6)

        high_price = item.get("high_price")
        low_price = item.get("low_price")
        if all(v is not None for v in [high_price, low_price, open_price]) and open_price != 0:
            item["volatility_percent"] = round((high_price - low_price) / open_price * 100, 6)
        else:
            item["volatility_percent"] = None

        processed.append(item)

    logger.info(f"[Indices] Обработано {len(processed)} индексов за {time.time() - start:.2f} сек")
    return processed


async def update_indexes():
    """Полный цикл обновления индексов: запрос → обработка → сохранение."""
    logger.info("[Indexes] Запуск сбора данных...")
    start_time = time.time()

    async with MOEXClient() as client:
        try:
            raw_data = await client.get_indexes()
            if not raw_data or 'securities' not in raw_data:
                logger.warning("[Indexes] Пустой ответ от API")
                return

            processed_data = process_index_data(raw_data)
            if not processed_data:
                logger.warning("[Indexes] Нет данных для сохранения после обработки")
                return

            async with get_db() as db:
                await upsert_market_data(db, processed_data)

            duration = time.time() - start_time
            logger.info(f"[Indexes] ✅ Успешно сохранено {len(processed_data)} записей за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[Indexes] ❌ Ошибка: {e}", exc_info=True)