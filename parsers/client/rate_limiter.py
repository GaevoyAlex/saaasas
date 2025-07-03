import asyncio
import time
from collections import deque
from typing import Dict
import structlog
from config import Config

logger = structlog.get_logger()

class RateLimiter:
    def __init__(self):
        self.requests_per_minute = Config.RATE_LIMIT_REQUESTS
        self.window_seconds = Config.RATE_LIMIT_WINDOW
        self.request_times = deque()
        self.logger = logger.bind(component="RateLimiter")
    
    async def acquire(self):
        current_time = time.time()
        
 
        while self.request_times and current_time - self.request_times[0] >= self.window_seconds:
            self.request_times.popleft()
        
 
        if len(self.request_times) >= self.requests_per_minute:
            sleep_time = self.window_seconds - (current_time - self.request_times[0])
            if sleep_time > 0:
                self.logger.info("Rate limit reached, sleeping", sleep_time=sleep_time)
                await asyncio.sleep(sleep_time)
                return await self.acquire()
        
 
        self.request_times.append(current_time)
        
 
        await asyncio.sleep(2)
    
    def get_stats(self) -> Dict:
        current_time = time.time()
 
        recent_requests = sum(1 for req_time in self.request_times 
                            if current_time - req_time < self.window_seconds)
        
        return {
            'requests_in_window': recent_requests,
            'max_requests': self.requests_per_minute,
            'window_seconds': self.window_seconds,
            'remaining_requests': max(0, self.requests_per_minute - recent_requests)
        }