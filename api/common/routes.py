from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Literal
import logging

# Импорты DAO и сессии
from api.database.engine import get_session
from api.database.dao import BaseDao, CapitalizationDAO, CandlesDAO, CompanyDAO

# Схема ответа
from api.common.schemas import Forex, Company


# Настройка логгера
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

router = APIRouter(tags=["Common"])

@router.get("/forex", response_model=List[Forex])
async def get_currency(
    session: AsyncSession = Depends(get_session),
    page: int = Query(1, ge=1, description="Номер страницы, начиная с 1"),
    per_page: int = Query(40, le=100, description="Количество записей на страницу, максимум 100"),
):
    """Получить котировки валют от ЦБ"""
    forex = await BaseDao.get_page(session=session, instrument_type="forex", page=page, per_page=per_page)
    result = [
        {
            "id": index + 1,
            "secid": f"{valute.secid}/RUB",
            "last_price": valute.last_price,
            "shortname": valute.shortname,
            "logo_url": f"/valute_logo/{valute.secid}.svg"
        }
        for index, valute in enumerate(forex)
    ]
    return result


@router.get("/capitalization")
async def get_market_capitalization(
    session: AsyncSession = Depends(get_session),
    period: str = Query(
        default="1m",
        description="Период: 1d-день, 1w-неделя, 1m-месяц, 6m-полгода, ytd-с начала года, 1y-год"
    ),
):
    """Получить рыночную капитализацию"""
    return await CapitalizationDAO.get_capitalization(session=session, period=period)


Period = Literal["1w", "1m", "6m", "ytd", "1y", "all"]

@router.get("/candles")
async def get_candles_endpoint(
    ticker: str = Query(..., min_length=1, max_length=20, pattern=r"^[A-Z0-9_]+$"),
    period: Period = Query("1m", description="Период: 1w, 1m, 6m, ytd, 1y, all"),
    session: AsyncSession = Depends(get_session),
):
    """Получить свечи по тикеру из базы данных"""
    try:
        return await CandlesDAO.get_candles(session=session, ticker=ticker, period=period)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Unexpected error in /candles (ticker={ticker}, period={period})")
        raise HTTPException(status_code=500, detail="Ошибка при получении свечей")


@router.get("/companies/{secid}", response_model=Company)
async def get_info_companies_by_secid(
        secid: str,
        session: AsyncSession = Depends(get_session)):
    result = await CompanyDAO.get_company_info(session = session, secid = secid)
    return result