from typing import Optional
from pydantic import BaseModel
from datetime import date


class StockForTable(BaseModel):
    id: Optional[int] = None
    shortname: Optional[str] = None
    secid: Optional[str] = None
    last_price: Optional[float] = None
    change_percent: Optional[float] = None
    capitalization: Optional[float] = None

    model_config = {"from_attributes": True}