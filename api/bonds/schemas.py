from typing import Optional
from pydantic import BaseModel, model_validator
from datetime import date


class BondForTable(BaseModel):
    id: Optional[int]  = None
    secid: Optional[str] = None
    boardid: Optional[str] = None
    shortname: Optional[str] = None
    maturity_date: Optional[date] = None
    couponpercent: Optional[float] = None
    last_price: Optional[float] = None
    change_percent: Optional[float] = None
    effectiveyield: Optional[float] = None
    currency: Optional[str] = None

    model_config = {"from_attributes": True}


class BondEvent(BaseModel):
    id: int
    secid: str
    shortname: str
    currency: str = "SUR"
    event_type: str
    couponvalue: Optional[float] = None
    next_coupon_date: Optional[date] = None
    couponpercent: Optional[float] = None
    count_day: Optional[int] = None
    facevalue: Optional[float] = None
    maturity_date: Optional[date] = None
    boardid: Optional[str] = None
    last_price: Optional[float] = None
    change_percent: Optional[float] = None
    effectiveyield: Optional[float] = None

    # ============= Вычисляемое поле: дней до события =============
    @model_validator(mode='after')
    def compute_count_day(self):
        today = date.today()
        if self.event_type == "payment" and self.next_coupon_date:
            self.count_day = (self.next_coupon_date - today).days
        elif self.event_type == "repayment" and self.maturity_date:
            self.count_day = (self.maturity_date - today).days
        else:
            self.count_day = None
        return self

    model_config = {"from_attributes": True}


class BondFullInfo(BaseModel):
    secid: str
    shortname: str
    last_price: Optional[float] = None
    change_percent: Optional[float] = None
    isin: Optional[str] = None
    facevalue: Optional[float] = None
    maturity_date: Optional[date] = None
    couponpercent: Optional[float] = None
    couponperiod: Optional[float] = None
    accruedint: Optional[float] = None
    issuesizeplaced: Optional[float] = None
    duration_years: Optional[float] = None
    currency: Optional[str] = None
    full_price: Optional[float] = None
    effectiveyield: Optional[float] = None

    model_config = {"from_attributes": True}
