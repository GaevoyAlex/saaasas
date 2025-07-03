from typing import Dict, Any, List
from datetime import datetime
from abc import ABC, abstractmethod
import structlog
from ..client import DynamoDBClient
from config import Config

logger = structlog.get_logger()

class BaseRepository(ABC):
    def __init__(self):
        self.db_client = DynamoDBClient()
        self.logger = logger.bind(component=self.__class__.__name__)
    
    def _create_ttl(self, days: int) -> int:
        return int(datetime.utcnow().timestamp() + (days * 24 * 60 * 60))
    
    def _batch_save(self, items: List[Dict[str, Any]]) -> bool:
        if not items:
            return True
            
        batch_size = 25
        success_count = 0
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            if self.db_client.batch_write(batch):
                success_count += len(batch)
        
        total = len(items)
        success_rate = (success_count / total) * 100 if total > 0 else 0
        
        self.logger.info(
            "Batch save completed",
            total=total,
            successful=success_count,
            success_rate=f"{success_rate:.1f}%"
        )
        
        return success_count == total
    
    @abstractmethod
    def save_batch(self, models: List[Any]) -> bool:
        pass
    
    @abstractmethod
    def get_latest(self, entity_id: str) -> Dict[str, Any]:
        pass