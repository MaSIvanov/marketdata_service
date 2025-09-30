import time
import logging
from typing import List, Dict
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from scheduler.database.models import MarketData, MarketCap
from datetime import datetime

logger = logging.getLogger(__name__)
BATCH_SIZE = 1000

async def upsert_market_data(db: AsyncSession, data: List[Dict]):
    """
    Массовый upsert с замером времени выполнения.
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
            stmt = stmt.on_conflict_do_update(
                index_elements=['secid', 'boardid'],
                set_={key: getattr(stmt.excluded, key) for key in batch[0].keys()
                      if key not in ('secid', 'boardid')}
            )

            await db.execute(stmt)
            await db.flush()

            batch_duration = time.time() - batch_start
            logger.debug(f"Батч {i//BATCH_SIZE + 1}: {len(batch)} записей → {batch_duration:.3f} сек")

        await db.commit()

        total_duration = time.time() - start_time
        logger.info(f"Успешно upserted {total} записей за {total_duration:.3f} сек")

    except Exception as e:
        await db.rollback()
        total_duration = time.time() - start_time
        logger.error(f"Ошибка при upsert: {e} (время до ошибки: {total_duration:.3f} сек)")
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
        logger.error(f"❌ Ошибка при upsert капитализации: {e} (время: {duration:.3f} сек)")
        raise
