from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel

class CoinData(BaseModel):

    id: str
    name: str
    symbol: str
    current_price: float = 0
    market_cap: Optional[float] = None
    market_cap_rank: Optional[int] = None
    total_volume: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    price_change_24h: Optional[float] = None
    price_change_percentage_24h: Optional[float] = None
    market_cap_change_24h: Optional[float] = None
    market_cap_change_percentage_24h: Optional[float] = None
    circulating_supply: Optional[float] = None
    total_supply: Optional[float] = None
    max_supply: Optional[float] = None
    last_updated: str = ""
    
    @classmethod
    def from_coingecko(cls, data: Dict[str, Any]) -> 'CoinData':
        """Создаёт модель из данных CoinGecko API"""
        return cls(
            id=data.get('id', ''),
            name=data.get('name', ''),
            symbol=data.get('symbol', ''),
            current_price=data.get('current_price', 0),
            market_cap=data.get('market_cap'),
            market_cap_rank=data.get('market_cap_rank'),
            total_volume=data.get('total_volume'),
            high_24h=data.get('high_24h'),
            low_24h=data.get('low_24h'),
            price_change_24h=data.get('price_change_24h'),
            price_change_percentage_24h=data.get('price_change_percentage_24h'),
            market_cap_change_24h=data.get('market_cap_change_24h'),
            market_cap_change_percentage_24h=data.get('market_cap_change_percentage_24h'),
            circulating_supply=data.get('circulating_supply'),
            total_supply=data.get('total_supply'),
            max_supply=data.get('max_supply'),
            last_updated=data.get('last_updated', datetime.utcnow().isoformat())
        )

class MarketData(BaseModel):

    coin_id: str
    timestamp: int
    price: float
    market_cap: Optional[float] = None
    volume: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    created_at: str
    
    @classmethod
    def from_coin_data(cls, coin_data: CoinData) -> 'MarketData':

        return cls(
            coin_id=coin_data.id,
            timestamp=int(datetime.utcnow().timestamp()),
            price=coin_data.current_price,
            market_cap=coin_data.market_cap,
            volume=coin_data.total_volume,
            high_24h=coin_data.high_24h,
            low_24h=coin_data.low_24h,
            created_at=datetime.utcnow().isoformat()
        )