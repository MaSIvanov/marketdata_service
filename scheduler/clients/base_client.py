# base_client.py
import httpx
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__name__)


class BaseHTTPClient:
    def __init__(
        self,
        base_url: str,
        client: Optional[httpx.AsyncClient] = None,
        timeout: float = 30.0,
        max_connections: int = 20,
        max_keepalive: int = 10,
        retry_attempts: int = 3,
        retry_min_wait: float = 2,
        retry_max_wait: float = 10,
    ):
        self.base_url = base_url
        self.retry_config = {
            "stop": stop_after_attempt(retry_attempts),
            "wait": wait_exponential(multiplier=1, min=retry_min_wait, max=retry_max_wait)
        }
        self.client = client or httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive
            )
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
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def _get_json(self, endpoint: str, params: dict = None) -> dict:
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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def _get_text(self, endpoint: str, params: dict = None) -> str:
        try:
            response = await self.client.get(endpoint, params=params)
            response.raise_for_status()
            return response.text
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP ошибка при запросе {endpoint}: {e}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Ошибка сети при запросе {endpoint}: {e}")
            raise