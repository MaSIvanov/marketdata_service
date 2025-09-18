from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from api.database.models import MarketData
from typing import List


class StockDAO:
    @staticmethod
    async def get_page(
            session: AsyncSession,
            page: int,
            per_page: int
    ) -> List[MarketData]:
        """
        Получает страницу акций с пагинацией

        Args:
            session: Асинхронная сессия БД
            page: Номер страницы (начинается с 1)
            per_page: Количество элементов на странице

        Returns:
            List[MarketData]: Список объектов MarketData
        """
        offset = (page - 1) * per_page

        try:
            result = await session.execute(
                select(MarketData)
                .where(MarketData.instrument_type == "stock")
                .order_by(MarketData.id)
                .offset(offset)
                .limit(per_page)
            )
            return result.scalars().all()
        except Exception as e:
            raise e