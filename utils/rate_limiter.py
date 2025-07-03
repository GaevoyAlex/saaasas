import asyncio
import time
from typing import Dict, List
from collections import deque
import structlog

logger = structlog.get_logger()

class SmartRateLimiter:
    def __init__(self, requests_per_minute: int = 10, burst_limit: int = 5):
        self.requests_per_minute = requests_per_minute
        self.burst_limit = burst_limit
        self.request_times = deque()
        self.burst_count = 0
        self.last_reset = time.time()
        
    async def acquire(self):
        current_time = time.time()
        
        if current_time - self.last_reset >= 60:
            self.burst_count = 0
            self.last_reset = current_time
        
        self.request_times = deque([t for t in self.request_times if current_time - t < 60])
        
        if len(self.request_times) >= self.requests_per_minute:
            sleep_time = 60 - (current_time - self.request_times[0])
            logger.info("Rate limit reached, sleeping", sleep_time=sleep_time)
            await asyncio.sleep(sleep_time)
            return await self.acquire()
        
        if self.burst_count >= self.burst_limit:
            await asyncio.sleep(2)
            self.burst_count = 0
        
        self.request_times.append(current_time)
        self.burst_count += 1
        
        if self.burst_count < self.burst_limit:
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(2)

class BatchProcessor:
    def __init__(self, batch_size: int = 250):
        self.batch_size = batch_size
    
    def create_batches(self, items: List[str], batch_size: int = None) -> List[List[str]]:
        size = batch_size or self.batch_size
        return [items[i:i + size] for i in range(0, len(items), size)]
    
    def prioritize_items(self, items: List[str], priorities: Dict[str, int]) -> List[str]:
        return sorted(items, key=lambda x: priorities.get(x, 0), reverse=True)

class DataCache:
    def __init__(self, ttl_seconds: int = 300):
        self.cache: Dict[str, Dict] = {}
        self.ttl = ttl_seconds
    
    def get(self, key: str) -> Dict:
        if key in self.cache:
            data, timestamp = self.cache[key]['data'], self.cache[key]['timestamp']
            if time.time() - timestamp < self.ttl:
                return data
            else:
                del self.cache[key]
        return None
    
    def set(self, key: str, data: Dict):
        self.cache[key] = {
            'data': data,
            'timestamp': time.time()
        }
    
    def clear_expired(self):
        current_time = time.time()
        expired_keys = [
            key for key, value in self.cache.items()
            if current_time - value['timestamp'] >= self.ttl
        ]
        for key in expired_keys:
            del self.cache[key]