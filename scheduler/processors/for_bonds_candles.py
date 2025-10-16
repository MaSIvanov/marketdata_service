import json
from datetime import date, timedelta
from typing import List, Dict, Any
import logging
import time

from scheduler.clients.moex_client import MOEXClient
from scheduler.database.dao import insert_daily_candles
from scheduler.database.engine import get_db

logger = logging.getLogger("scheduler.bond_candles")


def get_bond_candles(raw_data: Dict) -> List[Dict[str, Any]]:
    """
    Формирует список свечей для облигаций за вчерашний день.

    Использует:
      - PRICE из marketdata_yields как close
      - VALTODAY из marketdata как volume

    Облигации берутся ТОЛЬКО из marketdata_yields.
    Сопоставление с marketdata — по (SECID, BOARDID).

    Облигации с volume == 0 или None исключаются из результата.
    """
    target_date = date.today() - timedelta(days=1)

    # === 1. Парсим marketdata_yields (источник облигаций и цены) ===
    yields_data = raw_data.get("marketdata_yields", {}).get("data", [])
    yields_columns = raw_data.get("marketdata_yields", {}).get("columns", [])
    if not yields_data or not yields_columns:
        return []

    try:
        secid_y_idx = yields_columns.index("SECID")
        boardid_y_idx = yields_columns.index("BOARDID")
        price_idx = yields_columns.index("PRICE")
    except ValueError as e:
        raise ValueError(f"Не хватает колонок в marketdata_yields: {e}")

    # Собираем список облигаций из yields + их цены
    bond_list = []
    for row in yields_data:
        secid = row[secid_y_idx]
        boardid = row[boardid_y_idx]
        price_raw = row[price_idx]

        if price_raw is None:
            continue  # пропускаем, если нет цены

        try:
            close = float(price_raw)
        except (TypeError, ValueError):
            continue

        bond_list.append({
            "secid": secid,
            "boardid": boardid,
            "close": close
        })

    if not bond_list:
        return []

    # === 2. Парсим marketdata (для объёмов) ===
    market_data = raw_data.get("marketdata", {}).get("data", [])
    market_columns = raw_data.get("marketdata", {}).get("columns", [])
    if not market_data or not market_columns:
        # Если нет marketdata — все объёмы считаются 0 → исключаем все облигации
        return []

    try:
        secid_m_idx = market_columns.index("SECID")
        boardid_m_idx = market_columns.index("BOARDID")
        voltoday_idx = market_columns.index("VALTODAY")
    except ValueError as e:
        raise ValueError(f"Не хватает колонок в marketdata: {e}")

    # Создаём словарь для быстрого поиска объёма по (SECID, BOARDID)
    volume_dict = {}
    for row in market_data:
        secid = row[secid_m_idx]
        boardid = row[boardid_m_idx]
        voltoday = row[voltoday_idx]

        volume = 0
        if voltoday is not None:
            try:
                volume = int(voltoday)
                if volume < 0:
                    volume = 0
            except (TypeError, ValueError):
                volume = 0

        volume_dict[(secid, boardid)] = volume

    # === 3. Формируем итоговые свечи, исключая непоторговавшиеся облигации ===
    result = []
    for bond in bond_list:
        key = (bond["secid"], bond["boardid"])
        volume = volume_dict.get(key, 0)

        # Исключаем облигации с нулевым или отсутствующим объёмом
        if volume <= 0:
            continue

        result.append({
            "ticker": bond["secid"],
            "date": target_date,
            "close": bond["close"],
            "volume": volume
        })
    for i in result:
        if i["volume"] is None or i["volume"] == 0:
            print(i["ticker"])
    return result


async def update_bond_daily_candles():
    """
    Ежедневная задача: формирует и сохраняет свечи по облигациям за вчерашний день.
    Выполняется раз в сутки (например, в 00:35).
    """
    logger.info("[Bond Candles] 🕗 Запуск формирования свечей по облигациям...")

    start_time = time.time()

    async with MOEXClient() as client:
        try:
            # Предполагается, что этот метод возвращает:
            # {
            #   "marketdata_yields": { "columns": [...], "data": [...] },
            #   "marketdata": { "columns": [...], "data": [...] }
            # }
            raw_data = await client.get_bonds()
            if not raw_data:
                logger.warning("[Bond Candles] ❌ Пустой ответ от API")
                return

            candles = get_bond_candles(raw_data)
            if not candles:
                logger.warning("[Bond Candles] 📭 Нет валидных свечей для облигаций")
                return

            async with get_db() as db:
                await insert_daily_candles(db, candles)

            duration = time.time() - start_time
            logger.info(f"[Bond Candles] ✅ Сохранено {len(candles)} свечей за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[Bond Candles] ❌ Ошибка: {e}", exc_info=True)
            raise