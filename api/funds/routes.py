import logging
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from api.database.engine import get_session
from api.database.dao import BaseDao
from api.funds.schemas import FundForTable, FundFullInfo

# Простой логгер
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

router = APIRouter(prefix="/funds", tags=["Funds"])


@router.get("", response_model=List[FundForTable])
async def get_page(
        page: int = Query(1, ge=1, description="Номер страницы, начиная с 1"),
        per_page: int = Query(40, le=100, description="Количество записей на страницу, максимум 100"),
        session: AsyncSession = Depends(get_session)
):
    """Получить список акций с пагинацией и сквозной нумерацией."""
    try:
        funds = await BaseDao.get_page(session=session, instrument_type="fund", page=page, per_page=per_page)
        start_index = (page - 1) * per_page + 1
        result = [
            FundForTable.model_validate({
                **fund.__dict__,
                "id": start_index + index
            })
            for index, fund in enumerate(funds)
        ]
        return result

    except Exception as e:
        logger.error(f"Ошибка при получении акций: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/{secid}", response_model=FundFullInfo)
async def get_marketdata_fund(
        secid: str,
        session: AsyncSession = Depends(get_session)):
    """Получить рыночные данные по тикеру"""
    try:
        fund = await BaseDao.get_marketdata_by_secid(session=session, secid=secid.upper())
        return fund
    except Exception as e:
        logger.error(f"Ошибка при получении акций: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")
