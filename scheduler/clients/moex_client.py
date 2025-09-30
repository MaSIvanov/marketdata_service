from scheduler.clients.base_client import BaseHTTPClient
from typing import Dict

class MOEXClient(BaseHTTPClient):
    def __init__(self, client=None):
        super().__init__(
            base_url="https://iss.moex.com/iss",  # ← убраны пробелы в конце
            client=client,
            max_connections=20,
            max_keepalive=10
        )

    async def _fetch_securities(self, engine: str, market: str, board: str = None):
        if board:
            path = f"/engines/{engine}/markets/{market}/boards/{board}/securities.json"
        else:
            path = f"/engines/{engine}/markets/{market}/securities.json"
        return await self._get_json(path)

    async def get_stocks(self) -> Dict:
        """Акции на основном рынке (TQBR)"""
        return await self._fetch_securities("stock", "shares", "TQBR")

    async def get_bonds(self) -> Dict:
        """Облигации"""
        return await self._fetch_securities("stock", "bonds")

    async def get_indexes(self) -> Dict:
        """Индексы Московской биржи"""
        return await self._fetch_securities("stock", "index")

    async def get_tqtf_funds(self) -> Dict:
        """ETF на площадке TQTF"""
        return await self._fetch_securities("stock", "shares", "TQTF")

    async def get_tqif_funds(self) -> Dict:
        """Интервалы или фонды на TQIF (если актуально)"""
        return await self._fetch_securities("stock", "shares", "TQIF")

    async def get_capitalization(self) -> Dict:
        """Капитализация акций на Московской бирже"""
        path = "/statistics/engines/stock/capitalization.json"
        return await self._get_json(path)