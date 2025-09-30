from pydantic import BaseModel, HttpUrl

class ForexItem(BaseModel):
    id: int
    secid: str
    last_price: float
    shortname: str
    logo_url: HttpUrl | str  # если URL относительный, можно оставить str