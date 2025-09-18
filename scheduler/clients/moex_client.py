import httpx
from typing import Optional, Dict
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__name__)


class MOEXClient:
    BASE_URL = "https://iss.moex.com/iss"  # 🔥 Исправлено: убраны лишние пробелы!

    def __init__(self, client: Optional[httpx.AsyncClient] = None):
        self.client = client or httpx.AsyncClient(
            base_url=self.BASE_URL,
            timeout=30.0,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)
        )

    async def close(self):
        if self.client:
            await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def _get(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        try:
            response = await self.client.get(endpoint, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP ошибка при запросе {endpoint}: {e}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Ошибка сети при запросе {endpoint}: {e}")
            raise

    async def _fetch_securities(self, engine: str, market: str, board: str = None):
        if board:
            path = f"/engines/{engine}/markets/{market}/boards/{board}/securities.json"
        else:
            path = f"/engines/{engine}/markets/{market}/securities.json"
        return await self._get(path)

    # === Публичные методы ===

    async def get_stocks(self):
        return await self._fetch_securities("stock", "shares", "TQBR")

    async def get_bonds(self):
        return await self._fetch_securities("stock", "bonds")

    async def get_indexes(self):
        return await self._fetch_securities("stock", "index")

    async def get_tqtf_funds(self):
        return await self._fetch_securities("stock", "shares", "TQTF")

    async def get_tqif_funds(self):
        return await self._fetch_securities("stock", "shares", "TQIF")