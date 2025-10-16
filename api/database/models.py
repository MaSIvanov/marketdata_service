# api/database/models.py
from datetime import date, datetime
from typing import Optional
from sqlalchemy import (
    Integer,
    String,
    Numeric,
    DateTime,
    Date,
    UniqueConstraint,
    JSON,
    BIGINT,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class MarketData(Base):
    __tablename__ = "market_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    secid: Mapped[str] = mapped_column(String(36), nullable=False)
    boardid: Mapped[str] = mapped_column(String(12), nullable=False)
    instrument_type: Mapped[str] = mapped_column(String(10), nullable=False)

    shortname: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    currency: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    list_level: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    last_price: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)
    open_price: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)
    high_price: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)
    low_price: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)
    change_abs: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)
    change_percent: Mapped[Optional[float]] = mapped_column(Numeric(10, 6), nullable=True)
    volume: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    trades_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    volatility_percent: Mapped[Optional[float]] = mapped_column(Numeric(10, 6), nullable=True)
    capitalization: Mapped[Optional[float]] = mapped_column(Numeric(20, 2), nullable=True)
    change_capitalization: Mapped[Optional[float]] = mapped_column(Numeric(20, 2), nullable=True)

    annual_high: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)
    annual_low: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)

    maturity_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    couponpercent: Mapped[Optional[float]] = mapped_column(Numeric(10, 6), nullable=True)
    couponvalue: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)
    couponperiod: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    next_coupon_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    accruedint: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)
    full_price: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)
    effectiveyield: Mapped[Optional[float]] = mapped_column(Numeric(10, 6), nullable=True)
    duration_days: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    duration_years: Mapped[Optional[float]] = mapped_column(Numeric(10, 6), nullable=True)
    facevalue: Mapped[Optional[float]] = mapped_column(Numeric(18, 8), nullable=True)
    isin: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    lotsize: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    issuesize: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)
    issuesizeplaced: Mapped[Optional[int]] = mapped_column(BIGINT, nullable=True)

    __table_args__ = (
        UniqueConstraint("secid", "boardid", name="uq_market_data_secid_boardid"),
    )


class Candle(Base):
    __tablename__ = "candles"

    ticker: Mapped[str] = mapped_column(String(20), primary_key=True)
    date: Mapped[date] = mapped_column(Date, primary_key=True)
    close: Mapped[float] = mapped_column(Numeric(18, 8), nullable=False)
    volume: Mapped[int] = mapped_column(BIGINT, nullable=False)


class MarketCap(Base):
    __tablename__ = "market_caps"

    timestamp: Mapped[date] = mapped_column(Date, primary_key=True)
    cap: Mapped[float] = mapped_column(Numeric(24, 6), nullable=False)


class Coupons(Base):
    __tablename__ = "coupons"

    secid: Mapped[str] = mapped_column(String(51), primary_key=True)
    data: Mapped[dict] = mapped_column(JSON, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class Company(Base):
    __tablename__ = "companies"

    secid: Mapped[str] = mapped_column(Text, primary_key=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    founded: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    headquarters: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    employees: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ceo: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    link: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
