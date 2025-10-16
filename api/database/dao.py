from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, desc, asc
from api.database.models import MarketData, MarketCap, Candle, Company
from typing import List, Any, Dict
from api.database.models import Coupons
from api.bonds.utils import parse_bond_payments
from datetime import datetime, timedelta, date
import httpx

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
    def _downsample_records(records: List[Any], max_points: int = 50) -> List[Any]:
        """
        Уменьшает количество записей до max_points, равномерно распределяя их по времени.
        Всегда включает первую и последнюю запись.
        """
        n = len(records)
        if n <= max_points:
            return records

        # Всегда включаем первую и последнюю
        result = [records[0]]
        step = (n - 1) / (max_points - 1)

        # Добавляем промежуточные точки
        for i in range(1, max_points - 1):
            idx = int(round(i * step))
            # Защита от дублей и выхода за границы
            if idx < n and records[idx] != result[-1]:
                result.append(records[idx])

        # Гарантируем, что последняя запись включена
        if result[-1] != records[-1]:
            result.append(records[-1])
        return result

    @staticmethod
    async def get_capitalization(session: AsyncSession, period: str):
        today = date.today()

        if period == "1d":
            start_date = today - timedelta(days=1)
        elif period == "1w":
            start_date = today - timedelta(days=7)
        elif period == "1m":
            start_date = today - timedelta(days=30)
        elif period == "6m":
            start_date = today - timedelta(days=180)
        elif period == "1y":
            start_date = today - timedelta(days=365)
        elif period == "ytd":
            start_date = date(today.year, 1, 1)
        else:
            raise ValueError(f"Unsupported period: {period}")

        stmt = (
            select(MarketCap)
            .where(MarketCap.timestamp >= start_date)
            .order_by(MarketCap.timestamp)
        )
        result = await session.execute(stmt)
        records = result.scalars().all()

        if not records:
            return {
                "data": [],
                "current": None,
                "change_abs": None,
                "change_pct": None
            }

        # Применяем downsample только для больших периодов
        if period in ("6m", "1y", "ytd"):
            records = CapitalizationDAO._downsample_records(records, max_points=50)

        current = records[-1].cap
        first = records[0].cap

        change_abs = float(current - first)
        change_pct = float((current - first) / first * 100) if first != 0 else 0.0

        data = [
            [r.timestamp.strftime('%Y-%m-%d'), float(r.cap)]
            for r in records
        ]

        return {
            "current": float(current),
            "change_abs": change_abs,
            "change_pct": change_pct,
            "data": data
        }


class CandlesDAO:
    @staticmethod
    def _downsample_records(records: List[Any], max_points: int = 200) -> List[Any]:
        """Уменьшает количество записей, сохраняя первую, последнюю и равномерно распределяя остальные."""
        n = len(records)
        if n <= max_points:
            return records

        result = [records[0]]
        step = (n - 1) / (max_points - 1)

        for i in range(1, max_points - 1):
            idx = int(round(i * step))
            if idx < n and records[idx] != result[-1]:
                result.append(records[idx])

        if result[-1] != records[-1]:
            result.append(records[-1])
        return result

    @staticmethod
    async def get_candles(session: AsyncSession, ticker: str, period: str):
        today = date.today()

        # Определяем начальную дату
        if period == "1w":
            start_date = today - timedelta(weeks=1)
        elif period == "1m":
            start_date = today - timedelta(days=30)
        elif period == "6m":
            start_date = today - timedelta(days=180)
        elif period == "ytd":
            start_date = date(today.year, 1, 1)
        elif period == "1y":
            start_date = today - timedelta(days=365)
        elif period == "all":
            start_date = None  # без ограничения по дате
        else:
            raise ValueError(f"Unsupported period: {period}")

        # Запрос данных
        stmt = select(Candle).where(Candle.ticker == ticker)
        if start_date is not None:
            stmt = stmt.where(Candle.date >= start_date)
        stmt = stmt.order_by(Candle.date)

        result = await session.execute(stmt)
        records = result.scalars().all()

        if not records:
            return {
                "data": [],
                "change_pct": 0.0
            }

        # Применяем даунсэмплинг ТОЛЬКО для "all"
        if period == "all":
            records = CandlesDAO._downsample_records(records, max_points=200)

        # Формируем данные для графика
        data = [
            [
                r.date.strftime('%Y-%m-%d'),
                float(r.close),
                int(r.volume)
            ]
            for r in records
        ]

        # Считаем изменение в процентах
        first_price = float(records[0].close)
        last_price = float(records[-1].close)
        change_pct = ((last_price - first_price) / first_price * 100) if first_price != 0 else 0.0

        return {
            "data": data,
            "change_pct": round(change_pct, 2)
        }


class CouponDAO:
    BONDIZATION_URL = "https://iss.moex.com/iss/securities/{secid}/bondization.json"
    CACHE_TTL_HOURS = 24

    @classmethod
    async def get_or_fetch_bond_payments(
        cls,
        session: AsyncSession,
        secid: str
    ) -> List[Dict[str, Any]]:
        """
        Возвращает список событий (купоны, оферты, амортизации, погашения) по secid.
        Использует локальный кеш (таблица bondization_cache). При отсутствии или устаревании —
        обращается к API Мосбиржи, сохраняет ответ и возвращает распарсенные данные.
        """
        # 1. Попытка получить из кеша
        result = await session.execute(
            select(Coupons).where(Coupons.secid == secid)
        )
        cached = result.scalar_one_or_none()

        now = datetime.utcnow()
        ttl = timedelta(hours=cls.CACHE_TTL_HOURS)
        should_refresh = (
            cached is None
            or (now - cached.updated_at) > ttl
        )

        if not should_refresh:
            return parse_bond_payments(cached.data)

        # 2. Запрос к внешнему API
        try:
            url = cls.BONDIZATION_URL.format(secid=secid)
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, params={"limit": "unlimited"})
                response.raise_for_status()
                raw_data = response.json()
        except Exception as e:
            # Fallback: если API недоступен, но есть старый кеш — вернуть его
            if cached:
                return parse_bond_payments(cached.data)
            raise RuntimeError(f"Failed to fetch bondization data for {secid}: {e}")

        # 3. Сохранение в кеш
        if cached:
            cached.data = raw_data
            cached.updated_at = now
        else:
            cached = Coupons(secid=secid, data=raw_data)
            session.add(cached)

        await session.commit()
        return parse_bond_payments(raw_data)


class CompanyDAO:
    @staticmethod
    async def get_company_info(session: AsyncSession, secid: str):
        result = await session.execute(select(Company).where(Company.secid == secid))
        return result.scalars().one_or_none()