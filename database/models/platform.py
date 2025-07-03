from datetime import datetime
from pydantic import BaseModel, Field

class Platform(BaseModel):
    id: str = Field(..., description="UUID")
    token_id: str = Field(..., description="FK(UUID)")
    name: str
    symbol: str
    token_address: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_deleted: bool = False