# scheduler/processors/for_stocks.py

import time
import logging
from contextlib import asynccontextmanager

from scheduler.clients.moex_client import MOEXClient
from scheduler.database.dao import upsert_market_data
from scheduler.database.engine import get_db

logger = logging.getLogger("scheduler.stocks")

# Маппинг полей из API → наша модель
FIELDS_MAP = {
    "SECID": "secid",
    "BOARDID": "boardid",
    "LAST": "last_price",
    "OPEN": "open_price",
    "HIGH": "high_price",
    "LOW": "low_price",
    "VALTODAY": "volume",
    "NUMTRADES": "trades_count",
    "ISSUECAPITALIZATION": "capitalization",
    "TRENDISSUECAPITALIZATION": "change_capitalization",
}

# Поля из securities (для доп. данных)
SEC_FIELDS_MAP = {
    "SHORTNAME": "shortname",
    "PREVPRICE": "prev_price",
    "CURRENCYID": "currency",
    "LISTLEVEL": "list_level",
}


def process_stock_data(raw_data):
    start = time.time()

    marketdata = raw_data["marketdata"]
    columns = marketdata["columns"]
    rows = marketdata["data"]
    col_idx = {col: idx for idx, col in enumerate(columns)}

    securities = raw_data["securities"]
    sec_columns = securities["columns"]
    sec_rows = securities["data"]
    sec_col_idx = {col: idx for idx, col in enumerate(sec_columns)}

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
                if value == "" or value is None:
                    value = None
                if moex_field == "LISTLEVEL":
                    value = int(value) if value not in (None, "") else None
                sec_data[local_field] = value
        secid_to_data[secid] = sec_data

    parsed = []
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

        for moex_field, local_field in FIELDS_MAP.items():
            if moex_field not in col_idx:
                continue
            value = row[col_idx[moex_field]]
            if value == "" or value is None:
                value = None
            item[local_field] = value

        sec_data = secid_to_data.get(secid, {})
        item["shortname"] = sec_data.get("shortname")
        item["currency"] = sec_data.get("currency")
        item["list_level"] = sec_data.get("list_level")

        last_price = item.get("last_price")
        prev_price = sec_data.get("prev_price")
        if last_price is not None and prev_price is not None and prev_price != 0:
            item["change_abs"] = round(last_price - prev_price, 8)
            item["change_percent"] = round((last_price - prev_price) / prev_price * 100, 6)
        else:
            item["change_abs"] = None
            item["change_percent"] = None

        open_price = item.get("open_price")
        high_price = item.get("high_price")
        low_price = item.get("low_price")
        if all(v is not None for v in [high_price, low_price, open_price]) and open_price != 0:
            item["volatility_percent"] = round((high_price - low_price) / open_price * 100, 6)
        else:
            item["volatility_percent"] = None

        parsed.append(item)

    logger.info(f"[Stocks] Обработано {len(parsed)} инструментов за {time.time() - start:.2f} сек")
    return parsed


async def update_stocks():
    """Полный цикл обновления акций: запрос → обработка → сохранение."""
    logger.info("[Stocks] Запуск сбора данных...")
    start_time = time.time()

    async with MOEXClient() as client:
        try:
            raw_data = await client.get_stocks()
            if not raw_data or 'securities' not in raw_data:
                logger.warning("[Stocks] Пустой ответ от API")
                return

            processed_data = process_stock_data(raw_data)
            if not processed_data:
                logger.warning("[Stocks] Нет данных для сохранения после обработки")
                return

            async with get_db() as db:
                await upsert_market_data(db, processed_data)

            duration = time.time() - start_time
            logger.info(f"[Stocks] ✅ Успешно сохранено {len(processed_data)} записей за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[Stocks] ❌ Ошибка: {e}", exc_info=True)