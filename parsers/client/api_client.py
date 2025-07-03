import aiohttp
import asyncio
from typing import Dict, Any, Optional, List
import structlog

from utils.exeption import APIException


from .rate_limiter import RateLimiter
from config import Config


logger = structlog.get_logger()

class CoinGeckoAPIClient:
    def __init__(self):
        self.base_url = Config.COINGECKO_API_URL
        self.api_key = Config.COINGECKO_API_KEY
        self.timeout = Config.REQUEST_TIMEOUT
        self.max_retries = Config.MAX_RETRIES
        self.rate_limiter = RateLimiter()
        self.logger = logger.bind(component="APIClient")
    
    async def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        await self.rate_limiter.acquire()
        
        url = f"{self.base_url}/{endpoint}"
        
        if params is None:
            params = {}
        
        if self.api_key:
            params['x_cg_demo_api_key'] = self.api_key
        
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'CoinGecko-Parser/2.0'
        }
        
        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                    async with session.get(url, params=params, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.logger.info("API request successful", endpoint=endpoint)
                            return data
                        elif response.status == 429:
                            self.logger.warning("Rate limit hit", attempt=attempt + 1)
                            await asyncio.sleep(10 * (attempt + 1))
                        else:
                            self.logger.error("API request failed", endpoint=endpoint, status=response.status)
                            
            except asyncio.TimeoutError:
                self.logger.warning("Request timeout", endpoint=endpoint, attempt=attempt + 1)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(5)
                    
            except Exception as e:
                self.logger.error("Request error", endpoint=endpoint, error=str(e), attempt=attempt + 1)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(5)
        
        raise APIException(f"Failed to fetch {endpoint} after {self.max_retries} attempts")
    
    async def get_exchanges_list(self) -> List[Dict[str, Any]]:
        return await self._make_request("exchanges/list")
    
    async def get_exchange_details(self, exchange_id: str) -> Dict[str, Any]:
        return await self._make_request(f"exchanges/{exchange_id}")
    
    async def get_coins_list(self, include_platform: bool = True) -> List[Dict[str, Any]]:
        params = {'include_platform': str(include_platform).lower()}
        return await self._make_request("coins/list", params)
    
    async def get_coins_markets(self, vs_currency: str = 'usd', page: int = 1, per_page: int = 250) -> List[Dict[str, Any]]:
        params = {
            'vs_currency': vs_currency,
            'order': 'market_cap_desc',
            'per_page': per_page,
            'page': page,
            'sparkline': 'false',
            'price_change_percentage': '1h,24h,7d,30d'
        }
        return await self._make_request("coins/markets", params)
    
    async def get_coin_details(self, coin_id: str) -> Dict[str, Any]:
        params = {
            'localization': 'false',
            'tickers': 'false',
            'market_data': 'true',
            'community_data': 'true',
            'developer_data': 'false',
            'sparkline': 'false'
        }
        return await self._make_request(f"coins/{coin_id}", params)
    
    async def ping(self) -> Dict[str, Any]:
        return await self._make_request("ping")
    
    async def validate_connection(self) -> bool:
        try:
            ping_data = await self.ping()
            return ping_data is not None and 'gecko_says' in ping_data
        except Exception:
            return False
    
    def get_rate_limit_stats(self) -> Dict[str, Any]:
        return self.rate_limiter.get_stats()