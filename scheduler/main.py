import asyncio
import signal
import logging
import sys
from contextlib import AsyncExitStack
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Импорты твоего проекта — не меняй, всё как у тебя
from scheduler.database.engine import engine, get_db
from scheduler.database.models import Base
from scheduler.database.dao import upsert_market_data
from scheduler.clients.moex_client import MOEXClient
from scheduler.processors.for_stocks import process_stock_data
from scheduler.processors.for_bonds import process_bonds_data
from scheduler.processors.for_indices import process_index_data
from scheduler.processors.for_funds import process_fund_data
from scheduler.settings import settings  # ← твои настройки, без изменений

# ========================
# 🔧 Настройка логирования
# ========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("scheduler")

# ========================
# 🧱 Глобальные переменные
# ========================
exit_stack = AsyncExitStack()
scheduler = AsyncIOScheduler()

# ========================
# 🛑 Graceful shutdown
# ========================
async def shutdown(signal_name: str = None):
    if signal_name:
        logger.info(f"🛑 Получен сигнал {signal_name}. Начинаем остановку...")

    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("✅ Планировщик остановлен")

    await exit_stack.aclose()
    await engine.dispose()
    logger.info("✅ Ресурсы освобождены")


# ========================
# ⏳ Ожидание доступности БД
# ========================
async def wait_for_db(max_retries: int = 30, delay: int = 2):
    """Ждёт, пока БД станет доступна. Использует settings.DATABASE_URL"""
    logger.info("⏳ Ожидание доступности БД...")
    temp_engine = create_async_engine(settings.DATABASE_URL)  # ← твои настройки

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


# ========================
# 🏗️ Создание таблиц (если не существуют)
# ========================
async def create_tables_if_not_exist():
    logger.info("🔄 Проверка и создание таблиц в БД...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("✅ Таблицы созданы или уже существуют")


# ========================
# 🔄 Универсальная задача обновления
# ========================
async def run_update_task(task_name: str, fetch_func, process_func):
    logger.info(f"[{task_name}] Запуск сбора данных...")
    start_time = asyncio.get_event_loop().time()

    async with MOEXClient() as client:
        try:
            raw_data = await fetch_func(client)
            if not raw_data or 'securities' not in raw_data:
                logger.warning(f"[{task_name}] Пустой ответ от API")
                return

            records_count = len(raw_data.get('securities', {}).get('data', []))
            logger.debug(f"[{task_name}] Получено {records_count} записей")

            processed_data = process_func(raw_data)
            if not processed_data:
                logger.warning(f"[{task_name}] Нет данных для сохранения после обработки")
                return

            async with get_db() as db:
                await upsert_market_data(db, processed_data)

            duration = asyncio.get_event_loop().time() - start_time
            logger.info(f"[{task_name}] ✅ Успешно сохранено {len(processed_data)} записей за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[{task_name}] ❌ Ошибка: {e}", exc_info=True)


# ========================
# 📈 Конкретные задачи
# ========================
async def update_stocks():
    await run_update_task("Stocks", lambda client: client.get_stocks(), process_stock_data)

async def update_bonds():
    await run_update_task("Bonds", lambda client: client.get_bonds(), process_bonds_data)

async def update_etf_tqtf():
    await run_update_task("ETF_TQTF", lambda client: client.get_tqtf_funds(), process_fund_data)

async def update_etf_tqif():
    await run_update_task("ETF_TQIF", lambda client: client.get_tqif_funds(), process_fund_data)

async def update_indexes():
    await run_update_task("Indexes", lambda client: client.get_indexes(), process_index_data)


# ========================
# 🚦 Первоначальная загрузка (опционально)
# ========================
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
        update_indexes()
    ]

    for task in tasks:
        try:
            await task
        except Exception as e:
            logger.error(f"Ошибка в первоначальной загрузке: {e}")

    logger.info("✅ Первоначальная загрузка завершена")


# ========================
# ⏱️ Настройка планировщика
# ========================
def setup_scheduler():
    try:
        # max_instances=1 — защита от параллельных запусков
        scheduler.add_job(update_stocks, IntervalTrigger(minutes=10), id="update_stocks", misfire_grace_time=300, max_instances=1)
        scheduler.add_job(update_bonds, IntervalTrigger(minutes=15), id="update_bonds", misfire_grace_time=300, max_instances=1)
        scheduler.add_job(update_etf_tqtf, IntervalTrigger(minutes=20), id="update_etf_tqtf", misfire_grace_time=600, max_instances=1)
        scheduler.add_job(update_etf_tqif, IntervalTrigger(minutes=30), id="update_etf_tqif", misfire_grace_time=300, max_instances=1)
        scheduler.add_job(update_indexes, IntervalTrigger(minutes=30), id="update_indexes", misfire_grace_time=900, max_instances=1)
        logger.info("✅ Задачи добавлены в планировщик")
    except Exception as e:
        logger.error(f"❌ Ошибка настройки планировщика: {e}")
        raise


# ========================
# 🚀 Главная функция
# ========================
async def main():
    # Регистрация обработчиков сигналов
    signals = (signal.SIGINT, signal.SIGTERM)
    for sig in signals:
        asyncio.get_event_loop().add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown(s.name))
        )

    try:
        # 1. Ждём доступности БД — используем settings.DATABASE_URL
        await wait_for_db()

        # 2. Создаём таблицы
        await create_tables_if_not_exist()

        # 3. Первоначальная загрузка (если включена в настройках)
        await initial_load()

        # 4. Настраиваем и запускаем шедулер
        setup_scheduler()
        scheduler.start()
        logger.info("🚀 Шедулер запущен и работает. Ожидание задач...")

        # 5. Health-check loop — интервал из settings
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


# ========================
# ▶️ Запуск
# ========================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Ручная остановка")
    except Exception as e:
        logger.error(f"💥 Необработанная ошибка: {e}", exc_info=True)
        sys.exit(1)