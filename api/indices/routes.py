import logging
from fastapi import APIRouter, Query, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Literal

from api.database.engine import get_session
from api.database.dao import BaseDao, IndexDAO
from api.indices.schemas import IndexForTable, IndexFullInfo

# Простой логгер
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

router = APIRouter(prefix="/indexes", tags=["Indexes"])


@router.get("/top", response_model=list[IndexForTable])
async def get_indices(
        type: str = Query(default="main", description="main, sector, rising, falling, volume, volatility"),
        session: AsyncSession = Depends(get_session)
):
    """Получить топ индексов."""
    try:
        indexes = await IndexDAO.get_top_indexes(session=session, type=type)
        result = [
            IndexForTable.model_validate({
                **ins.__dict__,
                "id": index + 1
            })
            for index, ins in enumerate(indexes)
        ]

        return result

    except Exception as e:
        logger.error(f"Ошибка при получении индексов: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("", response_model=List[IndexForTable])
async def get_page(
        page: int = Query(1, ge=1),
        per_page: int = Query(40, le=100),
        session: AsyncSession = Depends(get_session)
):
    """Получить список облигаций с пагинацией и сквозной нумерацией."""
    try:
        indexes = await BaseDao.get_page(session=session, instrument_type="index", page=page, per_page=per_page)
        start_index = (page - 1) * per_page + 1
        result = [
            IndexForTable.model_validate({
                **ins.__dict__,
                "id": start_index + index
            })
            for index, ins in enumerate(indexes)
        ]

        return result

    except Exception as e:
        logger.error(f"Ошибка при получении индексов: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")


@router.get("/{secid}", response_model=IndexFullInfo)
async def get_marketdata_stock(secid: str,
                               session: AsyncSession = Depends(get_session)
                               ):
    """Получить рыночные данные по тикеру"""
    try:
        index = await BaseDao.get_marketdata_by_secid(session=session, secid=secid.upper())
        return index
    except Exception as e:
        logger.error(f"Ошибка при получении индексов: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")
