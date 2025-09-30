import xml.etree.ElementTree as ET
from typing import List, Dict
from scheduler.clients.base_client import BaseHTTPClient


def parse_cbr_xml(xml_text: str) -> List[Dict]:
    root = ET.fromstring(xml_text)
    currencies = []

    for valute in root.findall('Valute'):
        char_code = valute.findtext("CharCode")
        value_str = valute.findtext("Value")
        if not char_code or not value_str:
            continue

        try:
            value = round(float(value_str.replace(",", ".")), 2)
        except ValueError:
            continue

        currencies.append({
            "secid": char_code,
            "shortname": valute.findtext("Name"),
            "boardid": "CBR",
            "last_price": value,
            "instrument_type": "forex"
        })

    return currencies


class CBRClient(BaseHTTPClient):
    def __init__(self, client=None):
        super().__init__(
            base_url="https://cbr.ru",  # ✅ Без пробелов!
            client=client,
            max_connections=10,
            max_keepalive=5
        )

    async def get_currency_today(self) -> List[Dict]:
        xml = await self._get_text("/scripts/XML_daily.asp")
        return parse_cbr_xml(xml)