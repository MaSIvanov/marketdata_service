# markets/stocks/router.py

import logging
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from api.database.engine import get_session
from api.database.dao import BaseDao, StockDAO
from api.stocks.schemas import StockForTable, StockForTop, StockFullInfo

# Простой логгер
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

router = APIRouter(prefix="/stocks", tags=["Stocks"])


@router.get("", response_model=List[StockForTable])
async def get_page(
        page: int = Query(1, ge=1, description="Номер страницы, начиная с 1"),
        per_page: int = Query(40, le=100, description="Количество записей на страницу, максимум 100"),
        session: AsyncSession = Depends(get_session)
):
    """Получить список акций с пагинацией и сквозной нумерацией."""
    try:
        stocks = await BaseDao.get_page(session=session, instrument_type="stock", page=page, per_page=per_page)
        start_index = (page - 1) * per_page + 1
        result = [
            StockForTable.model_validate({
                **stock.__dict__,
                "id": start_index + index
            })
            for index, stock in enumerate(stocks)
        ]
        return result

    except Exception as e:
        logger.error(f"Ошибка при получении акций: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")



@router.get("/top", response_model=List[StockForTop])
async def get_top_stocks(
    type: str = Query(
        ...,
        description="Тип топа: 'volatility', 'volume', 'rising', 'falling'"
    ),
    limit: int = Query(5, ge=1, le=10, description="Количество записей в топе, максимум 10"),
    session: AsyncSession = Depends(get_session)
):
    """Получить топ акций по выбранному типу."""
    try:
        stocks = await StockDAO.get_top_stocks(session=session, type=type, limit=limit)
        result = [
            StockForTop.model_validate({
                **stock.__dict__,
                "id": index + 1
            })
            for index, stock in enumerate(stocks)
        ]
        return result

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Ошибка при получении топа акций: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

@router.get("/{secid}", response_model=StockFullInfo)
async def get_marketdata_stock(secid: str,
                               session: AsyncSession = Depends(get_session)
                               ):
    """Получить рыночные данные по тикеру"""
    try:
        stock = await BaseDao.get_marketdata_by_secid(session=session, secid=secid.upper())
        return stock
    except Exception as e:
        logger.error(f"Ошибка при получении акций: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")