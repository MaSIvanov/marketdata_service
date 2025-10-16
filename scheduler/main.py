# scheduler/main.py

import asyncio
import signal
import logging
import sys
from contextlib import AsyncExitStack
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Импорты обновлённых задач — теперь они содержат полную логику внутри
from scheduler.processors.for_stocks import update_stocks
from scheduler.processors.for_bonds import update_bonds
from scheduler.processors.for_funds import update_etf_tqtf, update_etf_tqif
from scheduler.processors.for_indices import update_indexes
from scheduler.processors.for_currencies import update_currencies
from scheduler.processors.for_capitalization import update_capitalization
from scheduler.processors.for_stocks_candles import update_daily_candles
from scheduler.processors.for_bonds_candles import update_bond_daily_candles
from scheduler.processors.for_indices_candles import update_indices_daily_candles
from scheduler.processors.for_funds_candles import update_tqif_candles, update_tqtf_candles
# Базовые компоненты
from scheduler.database.engine import engine
from scheduler.settings import settings
import pytz

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("scheduler")


exit_stack = AsyncExitStack()
moscow_tz = pytz.timezone('Europe/Moscow')
scheduler = AsyncIOScheduler(timezone=moscow_tz)


async def shutdown(signal_name: str = None):
    if signal_name:
        logger.info(f"🛑 Получен сигнал {signal_name}. Начинаем остановку...")

    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("✅ Планировщик остановлен")

    await exit_stack.aclose()
    await engine.dispose()
    logger.info("✅ Ресурсы освобождены")


async def wait_for_db(max_retries: int = 30, delay: int = 2):
    logger.info("⏳ Ожидание доступности БД...")
    temp_engine = create_async_engine(settings.DATABASE_URL)

    for i in range(max_retries):
        try:
            async with temp_engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            logger.info("✅ БД доступна")
            await temp_engine.dispose()
            return
        except Exception as e:
            logger.warning(f"⚠️ БД недоступна (попытка {i + 1}/{max_retries}): {e}")
            await asyncio.sleep(delay)

    await temp_engine.dispose()
    raise RuntimeError("❌ БД не стала доступна за отведённое время")


async def initial_load():
    if not settings.SCHEDULER_INITIAL_LOAD:
        logger.info("⏭️  Пропускаем первоначальную загрузку (настройка)")
        return

    logger.info("🔄 Запуск первоначальной загрузки...")
    tasks = [
        update_stocks(),
        update_bonds(),
        update_etf_tqtf(),
        update_etf_tqif(),
        update_indexes(),
        update_currencies(),
        update_capitalization(),
    ]

    for task in tasks:
        try:
            await task
        except Exception as e:
            logger.error(f"Ошибка в первоначальной загрузке: {e}")

    logger.info("✅ Первоначальная загрузка завершена")


def setup_scheduler():
    try:
        scheduler.add_job(update_stocks, IntervalTrigger(minutes=10), id="update_stocks", misfire_grace_time=300, max_instances=1)
        scheduler.add_job(update_bonds, IntervalTrigger(minutes=15), id="update_bonds", misfire_grace_time=300, max_instances=1)
        scheduler.add_job(update_etf_tqtf, IntervalTrigger(minutes=20), id="update_etf_tqtf", misfire_grace_time=600, max_instances=1)
        scheduler.add_job(update_etf_tqif, IntervalTrigger(minutes=30), id="update_etf_tqif", misfire_grace_time=300, max_instances=1)
        scheduler.add_job(update_indexes, IntervalTrigger(minutes=30), id="update_indexes", misfire_grace_time=900, max_instances=1)
        scheduler.add_job(update_currencies, IntervalTrigger(hours=1), id="update_currencies", misfire_grace_time=1800, max_instances=1)
        scheduler.add_job(update_capitalization, IntervalTrigger(hours=1), id="update_capitalization", misfire_grace_time=1800, max_instances=1)
        # === Ежедневные свечи — со вторника по субботу, 00:30–00:34 MSK ===
        scheduler.add_job(
            update_tqif_candles,
            CronTrigger(hour=0, minute=30, day_of_week="tue-sat", timezone=moscow_tz),
            id="tqif_daily_candles",
            misfire_grace_time=7200,
            max_instances=1
        )
        scheduler.add_job(
            update_tqtf_candles,
            CronTrigger(hour=0, minute=31, day_of_week="tue-sat", timezone=moscow_tz),
            id="tqtf_daily_candles",
            misfire_grace_time=7200,
            max_instances=1
        )
        scheduler.add_job(
            update_bond_daily_candles,
            CronTrigger(hour=0, minute=32, day_of_week="tue-sat", timezone=moscow_tz),
            id="bond_daily_candles",
            misfire_grace_time=7200,
            max_instances=1
        )
        scheduler.add_job(
            update_indices_daily_candles,
            CronTrigger(hour=0, minute=33, day_of_week="tue-sat", timezone=moscow_tz),
            id="index_daily_candles",
            misfire_grace_time=7200,
            max_instances=1
        )
        scheduler.add_job(
            update_daily_candles,
            CronTrigger(hour=0, minute=34, timezone=moscow_tz),
            id="daily_candles",
            misfire_grace_time=7200,
            max_instances=1
        )
    except Exception as e:
        logger.error(f"❌ Ошибка настройки планировщика: {e}")
        raise


async def main():
    signals = (signal.SIGINT, signal.SIGTERM)
    for sig in signals:
        asyncio.get_event_loop().add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown(s.name))
        )

    try:
        await wait_for_db()
        await initial_load()

        setup_scheduler()
        scheduler.start()
        logger.info("🚀 Шедулер запущен и работает. Ожидание задач...")

        while True:
            await asyncio.sleep(settings.SCHEDULER_HEALTH_CHECK_INTERVAL)
            logger.debug("🫀 Health check - шедулер работает")

    except asyncio.CancelledError:
        logger.info("⏹️  Получен запрос на остановку")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}", exc_info=True)
        raise
    finally:
        await shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Ручная остановка")
    except Exception as e:
        logger.error(f"💥 Необработанная ошибка: {e}", exc_info=True)
        sys.exit(1)