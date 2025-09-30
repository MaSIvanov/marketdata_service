from pydantic import BaseModel
from typing import Optional

class FundForTable(BaseModel):
    id: Optional[int] = None
    shortname: Optional[str] = None
    secid: Optional[str] = None
    boardid: Optional[str] = None
    last_price: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[float] = None
    currency: Optional[str] = None


    model_config = {"from_attributes": True}


class FundFullInfo(BaseModel):
    shortname: Optional[str] = None
    secid: Optional[str] = None
    list_level: Optional[int] = None
    last_price: Optional[float] = None
    change_percent: Optional[float] = None
    change_abs: Optional[float] = None
    volatility_percent: Optional[float] = None
    volume: Optional[float] = None
    currency: Optional[str] = None
    high_price: Optional[float] = None
    low_price: Optional[float] = None

    model_config = {"from_attributes": True}