# scheduler/processors/for_funds.py

import time
import logging
from datetime import datetime
from scheduler.clients.moex_client import MOEXClient
from scheduler.database.dao import upsert_market_data
from scheduler.database.engine import get_db

logger = logging.getLogger("scheduler.funds")

# Маппинг полей из marketdata → наша модель
FUND_MARKETDATA_MAP = {
    "SECID": "secid",
    "BOARDID": "boardid",
    "LAST": "last_price",
    "OPEN": "open_price",
    "HIGH": "high_price",
    "LOW": "low_price",
    "VALTODAY": "volume",
    "NUMTRADES": "trades_count",
}

# Поля из securities
SEC_FIELDS_MAP = {
    "SHORTNAME": "shortname",
    "PREVPRICE": "prev_price",
    "FACEUNIT": "currency",
    "LISTLEVEL": "list_level",
}


def process_fund_data(raw_data):
    """
    Обрабатывает данные по фондам (ETF) с API Мосбиржи и приводит к общей модели.
    :param raw_data: dict — ответ от /iss/engines/stock/markets/shares/boards/TQTF|TQIF/securities.json
    :return: list[dict] — готово к вставке в market_data
    """
    start = time.time()

    market = raw_data.get("marketdata")
    if not market or "data" not in market:
        logger.warning("'marketdata' missing or invalid")
        return []

    m_columns = market["columns"]
    m_rows = market["data"]
    m_col_idx = {col: idx for idx, col in enumerate(m_columns)}

    securities = raw_data.get("securities")
    if not securities or "data" not in securities:
        logger.warning("'securities' missing or invalid")
        return []

    s_columns = securities["columns"]
    s_rows = securities["data"]
    s_col_idx = {col: idx for idx, col in enumerate(s_columns)}

    secid_idx = s_col_idx.get("SECID")
    if secid_idx is None:
        logger.error("'SECID' column not found in securities")
        return []

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

            if moex_field == "LISTLEVEL":
                try:
                    value = int(value) if value not in (None, "") else None
                except (ValueError, TypeError):
                    value = None

            sec_info[local_field] = value

        sec_info["shortname"] = sec_info.get("shortname") or f"Fund {secid}"
        secid_to_info[secid] = sec_info

    required_market_cols = ["SECID", "BOARDID"]
    if not all(col in m_col_idx for col in required_market_cols):
        logger.error("Required columns missing in marketdata")
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

        for moex_field, local_field in FUND_MARKETDATA_MAP.items():
            if moex_field not in m_col_idx:
                continue
            value = row[m_col_idx[moex_field]]
            if value == "" or value is None:
                value = None

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

        sec_info = secid_to_info.get(secid, {})
        item["shortname"] = sec_info.get("shortname")
        item["currency"] = sec_info.get("currency", "SUR")
        item["list_level"] = sec_info.get("list_level")
        prev_price = sec_info.get("prev_price")

        last_price = item.get("last_price")
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

    logger.info(f"[Funds] Обработано {len(parsed)} фондов за {time.time() - start:.2f} сек")
    return parsed


async def update_etf_tqtf():
    """Обновление ETF с площадки TQTF."""
    logger.info("[ETF_TQTF] Запуск сбора данных...")
    start_time = time.time()

    async with MOEXClient() as client:
        try:
            raw_data = await client.get_tqtf_funds()
            if not raw_data or 'securities' not in raw_data:
                logger.warning("[ETF_TQTF] Пустой ответ от API")
                return

            processed_data = process_fund_data(raw_data)
            if not processed_data:
                logger.warning("[ETF_TQTF] Нет данных для сохранения после обработки")
                return

            async with get_db() as db:
                await upsert_market_data(db, processed_data)

            duration = time.time() - start_time
            logger.info(f"[ETF_TQTF] ✅ Успешно сохранено {len(processed_data)} записей за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[ETF_TQTF] ❌ Ошибка: {e}", exc_info=True)


async def update_etf_tqif():
    """Обновление ETF с площадки TQIF."""
    logger.info("[ETF_TQIF] Запуск сбора данных...")
    start_time = time.time()

    async with MOEXClient() as client:
        try:
            raw_data = await client.get_tqif_funds()
            if not raw_data or 'securities' not in raw_data:
                logger.warning("[ETF_TQIF] Пустой ответ от API")
                return

            processed_data = process_fund_data(raw_data)
            if not processed_data:
                logger.warning("[ETF_TQIF] Нет данных для сохранения после обработки")
                return

            async with get_db() as db:
                await upsert_market_data(db, processed_data)

            duration = time.time() - start_time
            logger.info(f"[ETF_TQIF] ✅ Успешно сохранено {len(processed_data)} записей за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[ETF_TQIF] ❌ Ошибка: {e}", exc_info=True)