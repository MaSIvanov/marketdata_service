from typing import List, Literal
from datetime import date
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc, asc
from api.database.models import MarketData



class BaseDao:
    @staticmethod
    async def get_page(
            session: AsyncSession,
            instrument_type: str,
            page: int,
            per_page: int
    ) -> List[MarketData]:
        offset = (page - 1) * per_page
        try:
            result = await session.execute(
                select(MarketData)
                .where(MarketData.instrument_type == instrument_type)
                .order_by(MarketData.id)
                .offset(offset)
                .limit(per_page)
            )
            return result.scalars().all()
        except Exception as e:
            raise e

    @staticmethod
    async def get_marketdata_by_secid(session: AsyncSession, secid: str):
        try:
            result = await session.execute(
                select(MarketData)
                .where(MarketData.secid == secid)
            )
            return result.scalars().first()
        except Exception as e:
            raise e


class StockDAO:
    @staticmethod
    async def get_top_stocks(session: AsyncSession, type: str, limit: int):
        """
        Получить топ акций по типу: 'volatility', 'volume', 'rising', 'falling'
        """
        query = select(MarketData).where(MarketData.instrument_type == "stock")

        if type == "volatility":
            query = query.where(MarketData.volatility_percent.isnot(None))
            query = query.order_by(desc(MarketData.volatility_percent))

        elif type == "volume":
            query = query.where(MarketData.volume.isnot(None))
            query = query.order_by(desc(MarketData.volume))

        elif type == "rising":
            query = query.where(
                and_(
                    MarketData.change_percent.isnot(None),
                    MarketData.change_percent != 0
                )
            )
            query = query.order_by(desc(MarketData.change_percent))

        elif type == "falling":
            query = query.where(
                and_(
                    MarketData.change_percent.isnot(None),
                    MarketData.change_percent != 0
                )
            )
            query = query.order_by(asc(MarketData.change_percent))

        else:
            raise ValueError(f"Неизвестный тип: {type}. Допустимые: volatility, volume, rising, falling")

        query = query.limit(limit)
        result = await session.execute(query)
        return result.scalars().all()



class BondDAO:
    @staticmethod
    async def get_events(session: AsyncSession, type: str, limit: int):
        """
        Получение событий по типу: выплаты купонов или погашения.
        """
        today = date.today()
        if type == "payment":
            stmt = (
                select(MarketData)
                .where(MarketData.next_coupon_date >= today)
                .order_by(asc(MarketData.next_coupon_date))
                .limit(limit)
            )
        elif type == "repayment":
            stmt = (
                select(MarketData)
                .where(MarketData.maturity_date >= today)
                .order_by(asc(MarketData.maturity_date))
                .limit(limit)
            )

        result = await session.execute(stmt)
        bonds = result.scalars().all()

        for bond in bonds:
            bond.event_type = type

        return bonds

    @staticmethod
    async def get_top_bonds(
            session: AsyncSession,
            type: str,
            limit: int
            ) -> List[MarketData]:
            today = date.today()

            base_filter = and_(
                MarketData.maturity_date > today,  # Ещё не погашены
                MarketData.list_level == 1,  # Высший уровень листинга
            )

            # Дополнительные условия для каждого типа запроса
            if type == "liquidity":
                stmt = (
                    select(MarketData)
                    .where(base_filter)
                    .order_by(desc(MarketData.volume))
                    .limit(limit)
                )
            elif type == "duration":
                stmt = (
                    select(MarketData)
                    .where(base_filter)
                    .order_by(desc(MarketData.duration_years))
                    .limit(limit)
                )
            elif type == "discount":
                # Только облигации с ценой ниже номинала
                discount_filter = and_(
                    base_filter,
                    MarketData.last_price < MarketData.facevalue,
                    MarketData.facevalue > 0,  # избегаем деления на ноль
                )
                # Сортируем по размеру дисконта в процентах
                stmt = (
                    select(MarketData)
                    .where(discount_filter)
                    .order_by(
                        desc(
                            ((MarketData.facevalue - MarketData.last_price) / MarketData.facevalue) * 100)
                    )
                    .limit(limit)
                )
            elif type == "coupon":
                stmt = (
                    select(MarketData)
                    .where(base_filter)
                    .order_by(desc(MarketData.couponpercent))
                    .limit(limit)
                )
            else:
                raise ValueError(
                    "Invalid type. Must be 'liquidity', 'duration', 'discount', or 'coupon'"
                )

            result = await session.execute(stmt)
            bonds = result.scalars().all()
            return bonds

    @staticmethod
    async def get_top_yields(session: AsyncSession, type: str, limit: int):
        # Определяем диапазон лет в зависимости от типа
        today = date.today()
        if type == "short":
            min_years, max_years = 0, 1
        elif type == "medium":
            min_years, max_years = 1, 5
        elif type == "long":
            min_years, max_years = 5, None
        else:
            raise ValueError("type must be 'short', 'medium', or 'long'")

        # Разница в днях между датой погашения и сегодняшней датой
        days_diff = MarketData.maturity_date - today
        years_left = days_diff / 365.25

        # Фильтр по сроку до погашения
        if max_years is not None:
            duration_filter = (years_left > min_years) & (years_left <= max_years)
        else:
            duration_filter = years_left > min_years

        # Основные условия отбора
        base_filter = (
                (MarketData.list_level == 1) &
                (MarketData.maturity_date > today) &
                (MarketData.effectiveyield > 0) &
                (MarketData.effectiveyield < 100) &
                (MarketData.accruedint.isnot(None)) &
                duration_filter
        )

        # Запрос: топ облигаций по доходности
        stmt = (
            select(MarketData)
            .where(base_filter)
            .order_by(MarketData.effectiveyield.desc())
            .limit(limit)
        )

        result = await session.execute(stmt)
        return result.scalars().all()


class IndexDAO:
    MAIN_INDEXES = {
        "IMOEX": "Индекс МосБиржи (главный)",
        "RTSI": "Индекс РТС (долларовый)",
        "MOEXBC": "Голубые фишки",
        "RGBITR": "Гос. облигации (полная доходность)",
        "RUCBICP": "Корп. облигации (ценовой)"
    }

    SECTOR_INDEXES = {
        "MOEXOG": "Нефть и газ",
        "MOEXFN": "Финансы",
        "MOEXMM": "Металлы и добыча",
        "MOEXCN": "Потребительский сектор",
        "MOEXTL": "Телекоммуникации",
        "MOEXIT": "IT-сектор",
        "MOEXEU": "Электроэнергетика",
        "MOEXTN": "Транспорт",
        "MOEXRE": "Недвижимость"
    }

    @staticmethod
    async def get_top_indexes(
        session: AsyncSession,
        type: str
    ):
        """
        Возвращает топ-индексы по различным критериям.
        Индексы определяются по MarketData.instrument_type == 'index'.
        """
        base_query = select(MarketData).where(MarketData.instrument_type == "index")

        if type == "main":
            result = await session.execute(
                base_query.where(MarketData.secid.in_(IndexDAO.MAIN_INDEXES.keys()))
            )
            return result.scalars().all()

        elif type == "sector":
            result = await session.execute(
                base_query.where(MarketData.secid.in_(IndexDAO.SECTOR_INDEXES.keys()))
            )
            return result.scalars().all()

        elif type in ("rising", "falling"):
            order = MarketData.change_percent.desc() if type == "rising" else MarketData.change_percent.asc()
            result = await session.execute(
                base_query.order_by(order).limit(5)
            )
            return result.scalars().all()

        elif type == "volume":
            result = await session.execute(
                base_query.order_by(MarketData.volume.desc()).limit(5)
            )
            return result.scalars().all()

        elif type == "volatility":
            result = await session.execute(
                base_query
                .where(MarketData.volatility_percent.isnot(None))
                .order_by(MarketData.volatility_percent.desc())
                .limit(5))
            return result.scalars().all()


        result = await session.execute(base_query)
        return result.scalars().all()

class CapitalizationDAO:



    @staticmethod
    async def get_capitalization(session: AsyncSession, period: str):
        if period == "1m":
            pass

