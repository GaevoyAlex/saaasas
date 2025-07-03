from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field
from decimal import Decimal

class Token(BaseModel):
    id: str = Field(..., description="UUID")
    exchanges: List[str] = []
    name: str
    symbol: str
    description: Optional[str] = None
    instagram: Optional[str] = None
    discord: Optional[str] = None
    website: Optional[str] = None
    facebook: Optional[str] = None
    reddit: Optional[str] = None
    twitter: Optional[str] = None
    repositories_link: Optional[str] = None
    whitepaper_link: Optional[str] = None
    tvl: Decimal = Decimal('0')
    avatar_image: str = ""
    security_audits: List[str] = []
    related_people: List[str] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_deleted: bool = False

class TokenStats(BaseModel):
    id: str = Field(..., description="UUID")
    token_id: str = Field(..., description="FK(UUID)")
    security_audits: List[str] = []
    market_cap: Optional[Decimal] = None
    trading_volume_24h: Optional[Decimal] = None
    token_max_supply: Optional[int] = None
    token_total_supply: Optional[int] = None
    transactions_count_30d: Optional[int] = None
    volume_1m_change_1m: Optional[int] = None
    volume_24h_change_24h: Optional[Decimal] = None
    price: Decimal = Decimal('0')
    fdv: Optional[Decimal] = None
    ath: Optional[Decimal] = None
    atl: Optional[Decimal] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_deleted: bool = False