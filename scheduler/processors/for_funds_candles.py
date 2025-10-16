# scheduler/processors/for_funds_candles.py

from datetime import datetime
from typing import List, Dict, Any
from scheduler.clients.moex_client import MOEXClient
from scheduler.database.dao import insert_daily_candles
from scheduler.database.engine import get_db
import logging
import time

logger = logging.getLogger("scheduler.funds_candles")


def get_funds_candles(raw_data: dict) -> List[Dict[str, Any]]:
    """
    Парсит данные по фондам (ETF/ПИФ) с MOEX и возвращает дневные свечи.
    Использует CLOSEPRICE, если доступен; иначе — LAST.
    Работает с любыми boardid (TQTF, TQIF и др.), если структура marketdata одинакова.

    :param raw_data: Ответ от MOEX API (секции securities + marketdata)
    :return: Список свечей: [{"ticker": str, "date": date, "close": float, "volume": int}, ...]
    """
    marketdata = raw_data.get("marketdata")
    if not marketdata or "data" not in marketdata or "columns" not in marketdata:
        raise ValueError("Отсутствует или повреждён раздел 'marketdata'")

    columns = marketdata["columns"]
    rows = marketdata["data"]

    # Обязательные поля (SYSTIME и SECID всегда нужны; CLOSEPRICE или LAST — опционально взаимозаменяемы)
    required_cols = {"SECID", "VOLTODAY", "SYSTIME"}
    if not required_cols.issubset(columns):
        missing = required_cols - set(columns)
        raise ValueError(f"В marketdata отсутствуют колонки: {missing}")

    secid_idx = columns.index("SECID")
    vol_idx = columns.index("VOLTODAY")
    systime_idx = columns.index("SYSTIME")

    # Опциональные индексы цены
    close_idx = columns.index("CLOSEPRICE") if "CLOSEPRICE" in columns else None
    last_idx = columns.index("LAST") if "LAST" in columns else None

    if close_idx is None and last_idx is None:
        raise ValueError("Ни CLOSEPRICE, ни LAST не найдены в marketdata")

    result = []
    for row in rows:
        # Проверка длины строки
        max_needed = max(
            secid_idx,
            vol_idx,
            systime_idx,
            close_idx if close_idx is not None else -1,
            last_idx if last_idx is not None else -1
        )
        if len(row) <= max_needed:
            continue

        try:
            secid = row[secid_idx]
            voltoday = row[vol_idx]
            systime_str = row[systime_idx]

            # Пропускаем без объёма
            if voltoday in (None, "", 0, "0"):
                continue

            # Парсим дату
            if not systime_str or not isinstance(systime_str, str):
                continue
            date_part = systime_str.split(" ")[0]
            candle_date = datetime.strptime(date_part, "%Y-%m-%d").date()

            # Определяем цену: сначала CLOSEPRICE, потом LAST
            closeprice = None
            if close_idx is not None:
                closeprice = row[close_idx]
            if closeprice is None and last_idx is not None:
                closeprice = row[last_idx]

            # Пропускаем, если всё ещё нет цены
            if closeprice is None:
                continue

            # Преобразуем значения
            close = float(closeprice)
            volume = int(float(voltoday))  # на случай "12345.0"
            if volume <= 0:
                continue

            result.append({
                "ticker": secid,
                "date": candle_date,
                "close": close,
                "volume": volume
            })

        except (ValueError, TypeError, IndexError, KeyError):
            # Пропускаем некорректные строки
            continue

    return result


async def update_funds_daily_candles(boardid: str):
    """
    Универсальная задача обновления дневных свечей по фондам на указанной площадке.
    """
    logger.info(f"[Funds Candles | {boardid}] 🕗 Запуск обновления свечей...")

    start_time = time.time()

    async with MOEXClient() as client:
        try:
            # Выбираем метод в зависимости от boardid
            if boardid == "TQTF":
                raw_data = await client.get_tqtf_funds()
            elif boardid == "TQIF":
                raw_data = await client.get_tqif_funds()
            else:
                raise ValueError(f"Не поддерживаемый boardid: {boardid}")

            if not raw_data or "marketdata" not in raw_data:
                logger.warning(f"[Funds Candles | {boardid}] ❌ Пустой ответ от API")
                return

            candles = get_funds_candles(raw_data)  # ← boardid больше не передаётся
            if not candles:
                logger.warning(f"[Funds Candles | {boardid}] 📭 Нет валидных свечей для сохранения")
                return

            async with get_db() as db:
                await insert_daily_candles(db, candles)

            duration = time.time() - start_time
            logger.info(f"[Funds Candles | {boardid}] ✅ Сохранено {len(candles)} свечей за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[Funds Candles | {boardid}] ❌ Ошибка: {e}", exc_info=True)
            raise


# === Wrapper-функции для планировщика ===

async def update_tqtf_candles():
    await update_funds_daily_candles("TQTF")

async def update_tqif_candles():
    await update_funds_daily_candles("TQIF")