import time
import logging
from scheduler.clients.moex_client import MOEXClient
from scheduler.database.dao import upsert_market_cap_data
from scheduler.database.engine import get_db

logger = logging.getLogger("scheduler.capitalization")

def process_capitalization(raw_data):
    result = []

    # capitalization → TRADEDATE = "2025-09-29"
    cap_section = raw_data.get("capitalization", {}).get("data")
    if cap_section:
        cap_value, trade_date = cap_section[0]
        result.append({"timestamp": trade_date, "cap": cap_value})

    # issuecapitalization → UPDATETIME = "2025-09-30 15:16:00"
    issue_section = raw_data.get("issuecapitalization", {}).get("data")
    if issue_section:
        cap_value, update_time = issue_section[0]
        date_only = update_time.split(" ")[0]  # ← берём только дату
        result.append({"timestamp": date_only, "cap": cap_value})

    return result

async def update_capitalization():
    """Полный цикл обновления капитализации: запрос → парсинг → сохранение."""
    logger.info("[Capitalization] Запуск сбора данных...")
    start_time = time.time()

    async with MOEXClient() as client:
        try:
            raw_data = await client.get_capitalization()
            if not raw_data or not isinstance(raw_data, dict):
                logger.warning("[Capitalization] Пустой или некорректный ответ от API")
                return

            processed_data = process_capitalization(raw_data)
            if not processed_data:
                logger.warning("[Capitalization] Нет данных для сохранения после обработки")
                return

            async with get_db() as db:
                await upsert_market_cap_data(db, processed_data)

            duration = time.time() - start_time
            logger.info(f"[Capitalization] ✅ Успешно сохранено {len(processed_data)} записей за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[Capitalization] ❌ Ошибка: {e}", exc_info=True)