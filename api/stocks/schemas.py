from typing import Optional
from pydantic import BaseModel


class StockForTable(BaseModel):
    id: Optional[int] = None
    shortname: Optional[str] = None
    secid: Optional[str] = None
    last_price: Optional[float] = None
    change_percent: Optional[float] = None
    capitalization: Optional[float] = None

    model_config = {"from_attributes": True}


class StockForTop(BaseModel):
    id: Optional[int] = None
    shortname: Optional[str] = None
    secid: Optional[str] = None
    last_price: Optional[float] = None
    change_percent: Optional[float] = None

    model_config = {"from_attributes": True}


class StockFullInfo(BaseModel):
    shortname: Optional[str] = None
    secid: Optional[str] = None
    last_price: Optional[float] = None
    change_percent: Optional[float] = None
    change_abs: Optional[float] = None
    capitalization: Optional[float] = None
    change_capitalization: Optional[float] = None
    volume: Optional[float] = None
    volatility_percent: Optional[float] = None
    open_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None


    model_config = {"from_attributes": True}