from typing import Optional
from pydantic import BaseModel, ConfigDict

class IndexForTable(BaseModel):
    id: Optional[int] = None
    secid: Optional[str] = None
    boardid: Optional[str] = None
    shortname: Optional[str] = None
    last_price: Optional[float] = None
    change_abs: Optional[float] = None
    change_percent: Optional[float]
    volatility_percent: Optional[float] = None
    currency: Optional[str] = None
    volume: Optional[int] = None  # BIGINT → int
    capitalization: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)

class IndexFullInfo(BaseModel):
    secid: Optional[str] = None
    boardid: Optional[str] = None
    shortname: Optional[str] = None
    last_price: Optional[float] = None
    change_abs: Optional[float] = None
    change_percent: Optional[float] = None
    volatility_percent: Optional[float] = None
    currency: Optional[str] = None
    volume: Optional[float] = None
    capitalization: Optional[float] = None

    high_price: Optional[float] = None
    low_price: Optional[float] = None
    annual_high: Optional[float] = None
    annual_low: Optional[float] = None

    model_config = {"from_attributes": True}