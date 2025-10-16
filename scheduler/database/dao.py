import time
import logging
from typing import List, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import func
from scheduler.database.models import MarketData, MarketCap, Candle
from datetime import datetime

logger = logging.getLogger(__name__)
BATCH_SIZE = 1000


async def upsert_market_data(db: AsyncSession, data: List[Dict]):
    """
    Массовый upsert с замером времени выполнения.
    Поля со значением None НЕ перезаписывают существующие значения в БД —
    сохраняется старое значение (защита от затирания данными NULL).
    """
    if not data:
        logger.info("Нет данных для upsert — пропускаем.")
        return

    total = len(data)
    logger.info(f"Начинаем upsert {total} записей (батч по {BATCH_SIZE})...")

    start_time = time.time()

    try:
        for i in range(0, total, BATCH_SIZE):
            batch_start = time.time()
            batch = data[i:i + BATCH_SIZE]

            stmt = insert(MarketData.__table__).values(batch)
            excluded = stmt.excluded
            table = MarketData.__table__

            # Обновляем только если новое значение НЕ NULL
            update_dict = {}
            for col in batch[0].keys():
                if col in ('secid', 'boardid'):
                    continue
                # COALESCE(new_value, old_value): если new IS NULL → оставить old
                update_dict[col] = func.coalesce(excluded[col], table.c[col])

            stmt = stmt.on_conflict_do_update(
                index_elements=['secid', 'boardid'],
                set_=update_dict
            )

            await db.execute(stmt)
            await db.flush()

            batch_duration = time.time() - batch_start
            logger.debug(f"Батч {i // BATCH_SIZE + 1}: {len(batch)} записей → {batch_duration:.3f} сек")

        await db.commit()

        total_duration = time.time() - start_time
        logger.info(f"Успешно upserted {total} записей за {total_duration:.3f} сек")

    except Exception as e:
        await db.rollback()
        total_duration = time.time() - start_time
        logger.error(f"Ошибка при upsert: {e} (время до ошибки: {total_duration:.3f} сек)", exc_info=True)
        raise


async def upsert_market_cap_data(db: AsyncSession, data: List[Dict]):
    """
    Upsert 1–2 записей рыночной капитализации.
    Ожидает список вида:
    [
        {"timestamp": "2025-09-29", "cap": 51761895103146.734},
        {"timestamp": "2025-09-30 15:16:00", "cap": 51897272299589.94}
    ]
    Все временные метки приводятся к дате (YYYY-MM-DD).
    """
    if not data:
        logger.info("Нет данных по капитализации — пропускаем upsert.")
        return

    start_time = time.time()

    try:
        # Нормализуем timestamp → date
        normalized = []
        for item in data:
            ts_str = item["timestamp"]
            # Извлекаем только дату
            date_str = ts_str.split(" ")[0]  # работает и для "2025-09-29", и для "2025-09-30 15:16:00"
            dt = datetime.strptime(date_str, "%Y-%m-%d").date()
            normalized.append({"timestamp": dt, "cap": item["cap"]})

        # Выполняем upsert
        stmt = insert(MarketCap).values(normalized)
        stmt = stmt.on_conflict_do_update(
            index_elements=["timestamp"],
            set_={"cap": stmt.excluded.cap}
        )
        await db.execute(stmt)
        await db.commit()

        duration = time.time() - start_time
        logger.info(f"✅ Успешно upserted {len(normalized)} записей капитализации за {duration:.3f} сек")

    except Exception as e:
        await db.rollback()
        duration = time.time() - start_time
        logger.error(f"❌ Ошибка при upsert капитализации: {e} (время: {duration:.3f} сек)", exc_info=True)
        raise


async def insert_daily_candles(db: AsyncSession, candles: List[Dict]) -> None:
    """
    Вставляет дневные свечи. Игнорирует дубликаты по (ticker, date).

    Ожидает список словарей вида:
    [
        {"ticker": "ABIO", "date": date(2025, 10, 11), "close": 66.24, "volume": 7470},
        ...
    ]
    """
    if not candles:
        logger.info("📭 Нет свечей для вставки — пропускаем.")
        return

    total = len(candles)
    logger.info(f"📥 Начинаем вставку {total} свечей (батч по {BATCH_SIZE})...")

    start_time = time.time()

    try:
        for i in range(0, total, BATCH_SIZE):
            batch = candles[i:i + BATCH_SIZE]
            batch_start = time.time()

            stmt = insert(Candle).values(batch)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["ticker", "date"]  # ← составной первичный ключ
            )
            await db.execute(stmt)
            await db.flush()

            batch_duration = time.time() - batch_start
            logger.debug(f"Батч {i // BATCH_SIZE + 1}: {len(batch)} свечей → {batch_duration:.3f} сек")

        await db.commit()
        total_duration = time.time() - start_time
        logger.info(f"✅ Успешно вставлено до {total} свечей за {total_duration:.3f} сек (дубликаты проигнорированы)")

    except Exception as e:
        await db.rollback()
        total_duration = time.time() - start_time
        logger.error(f"❌ Ошибка при вставке свечей: {e} (время до ошибки: {total_duration:.3f} сек)", exc_info=True)
        raise