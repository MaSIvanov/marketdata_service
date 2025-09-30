# scheduler/database/models.py
from sqlalchemy import (
    Column, Integer, String, Numeric, DateTime, Date,
    Index, MetaData, PrimaryKeyConstraint
)
from sqlalchemy.dialects.postgresql import BIGINT
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

# Рекомендуемое соглашение для именования constraints
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

metadata = MetaData(naming_convention=convention)
Base = declarative_base(metadata=metadata)

class MarketData(Base):
    __tablename__ = "market_data"

    id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    secid = Column(String(36), nullable=False)
    boardid = Column(String(12), nullable=False)
    instrument_type = Column(String(10), nullable=False)

    # Уникальный ключ и индексы
    __table_args__ = (
        Index('ix_secid_boardid', 'secid', 'boardid', unique=True),
        Index('ix_instrument_type', 'instrument_type'),
        Index('ix_isin', 'isin'),
        Index('ix_updated_at', 'updated_at'),  # Для очистки старых данных
    )

    # Общие атрибуты
    shortname = Column(String(255), nullable=True)
    currency = Column(String(10), nullable=True)
    list_level = Column(Integer, nullable=True)

    # Рыночные данные
    last_price = Column(Numeric(18, 8), nullable=True)
    open_price = Column(Numeric(18, 8), nullable=True)
    high_price = Column(Numeric(18, 8), nullable=True)
    low_price = Column(Numeric(18, 8), nullable=True)
    change_abs = Column(Numeric(18, 8), nullable=True)
    change_percent = Column(Numeric(10, 6), nullable=True)
    volume = Column(BIGINT, nullable=True)
    trades_count = Column(Integer, nullable=True)
    volatility_percent = Column(Numeric(10, 6), nullable=True)
    capitalization = Column(Numeric(20, 2), nullable=True)
    change_capitalization = Column(Numeric(20, 2), nullable=True)

    # Только для индексов
    annual_high = Column(Numeric(18, 8), nullable=True)
    annual_low = Column(Numeric(18, 8), nullable=True)

    # Только для облигаций
    maturity_date = Column(Date, nullable=True)
    couponpercent = Column(Numeric(10, 6), nullable=True)
    couponvalue = Column(Numeric(18, 8), nullable=True)
    couponperiod = Column(Integer, nullable=True)
    next_coupon_date = Column(Date, nullable=True)
    accruedint = Column(Numeric(18, 8), nullable=True)
    full_price = Column(Numeric(18, 8), nullable=True)
    effectiveyield = Column(Numeric(10, 6), nullable=True)
    duration_days = Column(Integer, nullable=True)
    duration_years = Column(Numeric(10, 6), nullable=True)
    facevalue = Column(Numeric(18, 8), nullable=True)
    isin = Column(String(50), nullable=True, index=True)

    # Дополнительные поля
    lotsize = Column(Integer, nullable=True)
    issuesize = Column(BIGINT, nullable=True)
    issuesizeplaced = Column(BIGINT, nullable=True)



class Candle(Base):
    __tablename__ = "candles"

    ticker = Column(String(20), nullable=False)
    time = Column(Date, nullable=False)
    open = Column(Numeric(18, 8), nullable=False)
    high = Column(Numeric(18, 8), nullable=False)
    low = Column(Numeric(18, 8), nullable=False)
    close = Column(Numeric(18, 8), nullable=False)
    volume = Column(Numeric(24, 8), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('ticker', 'time'),
    )


class MarketCap(Base):
    __tablename__ = "market_caps"

    timestamp = Column(Date, primary_key=True)  # ← только дата
    cap = Column(Numeric(24, 6), nullable=False)