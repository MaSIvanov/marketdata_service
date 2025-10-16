# scheduler/processors/for_indices_candles.py

import json
from datetime import datetime
from typing import List, Dict, Any
from scheduler.clients.moex_client import MOEXClient
from scheduler.database.dao import insert_daily_candles
from scheduler.database.engine import get_db
import logging
import time

logger = logging.getLogger("scheduler.indices_candles")

# Допустимые площадки для индексов
VALID_BOARDIDS = {"RTSI", "SNDX"}


def get_indices_candles(raw_data: dict) -> List[Dict[str, Any]]:
    """
    Парсит ответ от API Московской биржи по индексам и возвращает список свечей.
    Дата берётся из поля TRADEDATE (уже в формате YYYY-MM-DD).

    :param raw_data: Словарь с данными от API (ожидается структура как от /iss/engines/stock/markets/index/...)
    :return: Список словарей: [{"ticker": str, "date": date, "close": float, "volume": int}, ...]
    """
    marketdata = raw_data.get("marketdata")
    if not marketdata:
        raise ValueError("Отсутствует ключ 'marketdata' в ответе")

    columns = marketdata.get("columns")
    rows = marketdata.get("data")

    if not columns or not rows:
        return []

    # Находим индексы нужных колонок
    try:
        secid_idx = columns.index("SECID")
        boardid_idx = columns.index("BOARDID")
        tradedate_idx = columns.index("TRADEDATE")
        lastvalue_idx = columns.index("CURRENTVALUE")  # или можно использовать CURRENTVALUE — но LASTVALUE — официальное закрытие
        valtoday_idx = columns.index("VALTODAY")
    except ValueError as e:
        raise ValueError(f"Отсутствует обязательная колонка в marketdata: {e}")

    result = []
    for row in rows:
        try:
            secid = row[secid_idx]
            boardid = row[boardid_idx]

            # Фильтруем только нужные индексы
            if boardid not in VALID_BOARDIDS:
                continue

            tradedate_str = row[tradedate_idx]
            lastvalue = row[lastvalue_idx]
            valtoday = row[valtoday_idx]

            # Пропускаем, если нет даты или цены
            if not tradedate_str or lastvalue is None:
                continue

            # Пропускаем, если объём отсутствует, null, пустой или 0
            if valtoday in (None, "", 0, "0"):
                continue

            # Парсим дату
            try:
                candle_date = datetime.strptime(tradedate_str, "%Y-%m-%d").date()
            except ValueError:
                logger.warning(f"Некорректный формат даты TRADEDATE: {tradedate_str} для {secid}")
                continue

            # Преобразуем цену и объём
            try:
                close = float(lastvalue)
                volume = int(float(valtoday))  # MOEX иногда отдаёт объём как float-строку, например "12345.0"
                if volume <= 0:
                    continue
            except (TypeError, ValueError):
                continue

            result.append({
                "ticker": secid,
                "date": candle_date,
                "close": close,
                "volume": volume
            })

        except (IndexError, KeyError, TypeError) as e:
            # Пропускаем битые строки
            continue

    return result


async def update_indices_daily_candles():
    """
    Ежедневная задача: получает данные по индексам с MOEX и сохраняет дневные свечи.
    Выполняется раз в сутки (например, в 00:30).
    """
    logger.info("[Indices Candles] 🕗 Запуск ежедневного обновления свечей по индексам...")

    start_time = time.time()

    async with MOEXClient() as client:
        try:
            # Получаем данные по индексам (тот же эндпоинт, что и в for_indices.py)
            raw_data = await client.get_indexes()
            if not raw_data or "marketdata" not in raw_data:
                logger.warning("[Indices Candles] ❌ Пустой или некорректный ответ от API")
                return

            # Парсим свечи
            candles = get_indices_candles(raw_data)
            if not candles:
                logger.warning("[Indices Candles] 📭 Нет валидных свечей для сохранения")
                return

            # Сохраняем в БД
            async with get_db() as db:
                await insert_daily_candles(db, candles)

            duration = time.time() - start_time
            logger.info(f"[Indices Candles] ✅ Сохранено {len(candles)} свечей за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[Indices Candles] ❌ Ошибка при обновлении свечей: {e}", exc_info=True)
            raise