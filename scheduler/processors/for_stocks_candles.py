import json
from datetime import date, timedelta
from typing import List, Dict, Any
from scheduler.clients.moex_client import MOEXClient
from scheduler.database.dao import insert_daily_candles  # ← твоя новая функция вставки
from scheduler.database.engine import get_db
import logging
import time

logger = logging.getLogger("scheduler.candles")

def get_stocks_candles(raw_data: str) -> List[Dict[str, Any]]:
    """
    Парсит ответ от API Московской биржи и возвращает список свечей за вчерашний день.

    :param raw_data: Строка JSON с данными от API (должна содержать ключ "marketdata")
    :return: Список словарей с ключами: ticker, date, close, volume
    """
    try:
        data = json.loads(raw_data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Некорректный JSON: {e}")

    marketdata = data.get("marketdata")
    if not marketdata:
        raise ValueError("Отсутствует ключ 'marketdata' в ответе")

    columns = marketdata.get("columns")
    rows = marketdata.get("data")

    if not columns or not rows:
        return []

    # Находим индексы нужных колонок
    try:
        secid_idx = columns.index("SECID")
        last_idx = columns.index("LAST")
        voltoday_idx = columns.index("VOLTODAY")
    except ValueError as e:
        raise ValueError(f"Отсутствует обязательная колонка в marketdata: {e}")

    # Вчерашняя дата
    target_date = date.today() - timedelta(days=1)

    result = []
    for row in rows:
        try:
            ticker = row[secid_idx]
            last = row[last_idx]
            voltoday = row[voltoday_idx]

            # Пропускаем, если LAST — null (не торговалась)
            if last is None:
                continue

            close = float(last)
            volume = int(voltoday) if voltoday is not None else 0

            # 🔴 КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: пропускаем, если объём <= 0
            if volume <= 0:
                continue

            result.append({
                "ticker": ticker,
                "date": target_date,
                "close": close,
                "volume": volume
            })
        except (TypeError, ValueError, IndexError) as e:
            # Пропускаем некорректные строки
            continue

    return result


async def update_daily_candles():
    """
    Ежедневная задача: получает данные с MOEX за вчерашний день и сохраняет свечи.
    Выполняется раз в сутки в 00:30.
    """
    logger.info("[Candles] 🕗 Запуск ежедневного обновления свечей...")

    start_time = time.time()

    async with MOEXClient() as client:
        try:
            # 1. Получаем только marketdata с нужными колонками
            raw_data = await client.get_stocks()
            if not raw_data or "marketdata" not in raw_data:
                logger.warning("[Candles] ❌ Пустой или некорректный ответ от API")
                return

            # 2. Преобразуем в JSON-строку (если твоя функция требует str)
            raw_json_str = json.dumps(raw_data, ensure_ascii=False)

            # 3. Парсим в список свечей (с вчерашней датой и без null)
            candles = get_stocks_candles(raw_json_str)
            if not candles:
                logger.warning("[Candles] 📭 Нет валидных свечей для сохранения")
                return

            # 4. Сохраняем в БД (игнорируем дубликаты)
            async with get_db() as db:
                await insert_daily_candles(db, candles)

            duration = time.time() - start_time
            logger.info(f"[Candles] ✅ Сохранено {len(candles)} свечей за {duration:.2f} сек")

        except Exception as e:
            logger.error(f"[Candles] ❌ Ошибка при обновлении свечей: {e}", exc_info=True)
            raise