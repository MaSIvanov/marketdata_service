from fastapi import APIRouter, Depends, Query
from api.database.engine import get_session
from sqlalchemy.ext.asyncio import AsyncSession
from api.database.dao import BaseDao
from typing import List
from api.common.schemas import ForexItem


router = APIRouter(tags=["Common"])

@router.get("/forex", response_model=List[ForexItem])
async def get_currency(
        session: AsyncSession =Depends(get_session),
        page: int = Query(1, ge=1, description="Номер страницы, начиная с 1"),
        per_page: int = Query(40, le=100, description="Количество записей на страницу, максимум 100"),
):
    """Получить котировки валют от ЦБ"""
    forex = await BaseDao.get_page(session=session, instrument_type="forex", page=page, per_page=per_page)
    result = [
        {"id": index + 1,
         "secid": f"{valute.secid}/RUB",
         "last_price": valute.last_price,
         "shortname": valute.shortname,
         "logo_url":  f"/valute_logo/{valute.secid}.svg"} for index, valute in enumerate(forex)
    ]
    return result

@router.get("/capitalization")
async def get_market_capitalization(
        session: AsyncSession = Depends(get_session),
        period: int = Query(default="1m", description="Период, за который можно получить даннеы по капитализации")
):
    """Получить рыночную капитализацию"""
    pass