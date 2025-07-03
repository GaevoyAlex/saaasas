from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field
from decimal import Decimal

class Exchange(BaseModel):
    id: str = Field(..., description="UUID")
    name: str
    description: str
    islamic_account: str = ""
    facebook: str = ""
    youtube: str = ""
    instagram: str = ""
    medium: str = ""
    discord: str = ""
    website: str = ""
    twitter: str = ""
    reddit: bool = False
    repositories_link: str = ""
    avatar_image: str = ""
    native_token_symbol: str = ""
    security_audits: List[str] = []
    related_people: List[str] = []
    trading_pairs: List[str] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_deleted: bool = False

class ExchangeStats(BaseModel):
    id: str = Field(..., description="UUID")
    exchange_id: str = Field(..., description="FK(UUID)")
    inflows_1m: Optional[Decimal] = None
    inflows_1w: Optional[Decimal] = None
    inflows_24h: Optional[Decimal] = None
    trading_volume_1m: Optional[Decimal] = None
    trading_volume_1w: Optional[Decimal] = None
    trading_volume_24h: Optional[Decimal] = None
    visitors_7d: Optional[int] = None
    reserves: Optional[Decimal] = None
    fiat_supported: List[str] = []
    coins_count: int = 0
    liquidity_score: Decimal = Decimal('0')
    effective_liquidity_24h: Decimal = Decimal('0')
    percent_change_volume_24h: Decimal = Decimal('0')
    percent_change_volume_1m: Decimal = Decimal('0')
    percent_change_volume_7d: Decimal = Decimal('0')
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_deleted: bool = False