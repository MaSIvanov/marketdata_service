# markets/stocks/router.py
import logging
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from api.database.engine import get_session
from api.stocks.dao import StockDAO
from api.stocks.schemas import StockForTable

# Простой логгер
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

router = APIRouter(prefix="/stocks", tags=["Stocks"])


@router.get("", response_model=List[StockForTable])
async def get_stocks_page(
        page: int = Query(1, ge=1, description="Номер страницы, начиная с 1"),
        per_page: int = Query(40, le=100, description="Количество записей на страницу, максимум 100"),
        session: AsyncSession = Depends(get_session)
):
    """Получить список акций с пагинацией."""
    try:
        stocks = await StockDAO.get_page(session=session, page=page, per_page=per_page)
        return stocks

    except Exception as e:
        logger.error(f"Ошибка при получении акций: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")