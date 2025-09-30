# markets/bonds/router.py

import logging
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

from api.database.engine import get_session
from api.database.dao import BaseDao, BondDAO
from api.bonds.schemas import BondForTable, BondEvent, BondFullInfo

# Простой логгер
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

router = APIRouter(prefix="/bonds", tags=["Bonds"])


@router.get("", response_model=List[BondForTable])
async def get_page(
        page: int = Query(1, ge=1),
        per_page: int = Query(40, le=100),
        session: AsyncSession = Depends(get_session)
):
    """Получить список облигаций с пагинацией и сквозной нумерацией."""
    try:
        bonds = await BaseDao.get_page(session=session, instrument_type="bond", page=page, per_page=per_page)
        start_index = (page - 1) * per_page + 1
        result = [
            BondForTable.model_validate({
                **bond.__dict__,
                "id": start_index + index
            })
            for index, bond in enumerate(bonds)
        ]

        return result

    except Exception as e:
        logger.error(f"Ошибка при получении облигаций: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/events", response_model=list[BondEvent], tags=["Bonds"])
async def get_bond_events(
    type: str = Query(..., description="Тип события: 'repayment' или 'payment'"),
    limit: int = Query(default=10, le=20, description="Максимум 20 событий"),
    session: AsyncSession = Depends(get_session)
):
    """
    Получить события по облигациям: погашения или купонные выплаты.
    """
    if type not in ["repayment", "payment"]:
        raise HTTPException(status_code=400, detail="Invalid event type. Use 'repayment' or 'payment'.")

    bonds = await BondDAO.get_events(session=session, type=type, limit=limit)
    result = [
        BondEvent.model_validate({
            **bond.__dict__,
            "id": 1 + index
        })
        for index, bond in enumerate(bonds)
    ]
    return result

@router.get("/top", response_model=List[BondForTable])
async def get_top_bonds(
    session: AsyncSession = Depends(get_session),
    type: str = Query(default="liquidity", description="liquidity - по ликвидности, duration - по дюрации, discount - по дисконту, coupon - по купону"),
    limit: int = Query(default=5, le=20)
):
    """
    Топ облигаций по различным метрикам: ликвидность, дюрация, дисконт, купон.
    """
    bonds = await BondDAO.get_top_bonds(session=session, type=type, limit=limit)
    result = [
        BondForTable.model_validate({
            **bond.__dict__,
            "id": 1 + index
        })
        for index, bond in enumerate(bonds)
    ]
    return result

@router.get("/yields")
async def get_yields_bonds(
        session: AsyncSession = Depends(get_session),
        type: str = Query(default="long", description="long - долгосрочные, medium - среднесрочные, short - краткосрочные"),
        limit: int = Query(default=5, le=20, description="Не больше 20 бумаг")):
    """
    Топ облигаций по доходности.
    """
    bonds = await BondDAO.get_top_yields(session=session, type=type, limit=limit)
    result = [
        BondForTable.model_validate({
            **bond.__dict__,
            "id": 1 + index
        })
        for index, bond in enumerate(bonds)
    ]
    return result


@router.get("/{secid}", response_model=BondFullInfo)
async def get_marketdata_bond(
        secid: str,
        session: AsyncSession = Depends(get_session)
):
    """Получить рыночные данные по тикеру"""
    try:
        bond = await BaseDao.get_marketdata_by_secid(session=session, secid=secid.upper())
        return bond
    except Exception as e:
        logger.error(f"Ошибка при получении акций: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")