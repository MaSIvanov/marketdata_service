# scheduler/processors/for_currencies.py

import asyncio
import logging

from scheduler.clients.cbr_client import CBRClient
from scheduler.database.dao import upsert_market_data
from scheduler.database.engine import get_db

logger = logging.getLogger("scheduler.currencies")


async def update_currencies():
    """
    Полный цикл обновления валютных курсов от ЦБ РФ:
    запрос → обработка (в CBRClient уже возвращается готовый список) → сохранение.
    """
    logger.info("[Currencies] Запуск сбора данных...")
    start_time = asyncio.get_event_loop().time()

    async with CBRClient() as client:
        try:
            # CBRClient.get_currency_today() возвращает List[Dict], готовый к upsert
            raw_data = await client.get_currency_today()
            if not raw_data:
                logger.warning("[Currencies] Пустой ответ от CBR API")
                return

            logger.debug(f"[Currencies] Получено {len(raw_data)} валют")

            async with get_db() as db:
                await upsert_market_data(db, raw_data)

            duration = asyncio.get_event_loop().time() - start_time
            logger.info(f"[Currencies] ✅ Успешно сохранено {len(raw_data)} валют за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[Currencies] ❌ Ошибка: {e}", exc_info=True)