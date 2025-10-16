from pydantic import BaseModel, HttpUrl
from typing import Optional

class Forex(BaseModel):
    id: int
    secid: str
    last_price: float
    shortname: str
    logo_url: HttpUrl | str


class Company(BaseModel):
    secid: str
    description: Optional[str] = None
    founded: Optional[int] = None
    headquarters: Optional[str] = None
    employees: Optional[str] = None
    sector: Optional[str] = None
    ceo: Optional[str] = None
    link: Optional[str] = None