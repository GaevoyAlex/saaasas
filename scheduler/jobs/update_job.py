import asyncio
from datetime import datetime
import structlog
from parsers.main_parser import MainCoinGeckoParser
from database.repositories.token_repo import TokenRepository

logger = structlog.get_logger()

class QuickUpdateJob:
    def __init__(self):
        self.parser = MainCoinGeckoParser()
        self.token_repo = TokenRepository()
        self.logger = logger.bind(component="QuickUpdateJob")
        
        self.stats = {
            'last_run': None,
            'duration_seconds': 0,
            'prices_updated': 0,
            'api_requests_used': 0
        }
    
    async def execute(self):
        """Выполнить быстрое обновление цен"""
        start_time = datetime.utcnow()
        self.stats['last_run'] = start_time.isoformat()
        
        try:
            self.logger.info("Starting quick price update job")
            
            # Получаем быстрые обновления цен
            price_updates = await self.parser.parse_quick_prices()
            
            if not price_updates:
                self.logger.warning("No price updates received")
                return
            
            # Сохраняем обновления
            await self._save_price_updates(price_updates)
            
            # Обновляем статистику
            self._update_stats(price_updates, start_time)
            
            self.logger.info(
                "Quick update job completed successfully",
                duration_seconds=self.stats['duration_seconds'],
                prices_updated=self.stats['prices_updated']
            )
            
        except Exception as e:
            self.logger.error("Quick update job failed", error=str(e))
            self.stats['duration_seconds'] = (datetime.utcnow() - start_time).total_seconds()
            raise
    
    async def _save_price_updates(self, price_updates: list):
        """Сохранить обновления цен"""
        batch_size = 50
        successful_updates = 0
        
        for i in range(0, len(price_updates), batch_size):
            batch = price_updates[i:i + batch_size]
            
            # Сохраняем батч быстрых обновлений
            for update in batch:
                try:
                    success = await asyncio.get_event_loop().run_in_executor(
                        None,
                        self.token_repo.quick_price_update,
                        update['symbol'],
                        update['price'],
                        update.get('market_cap')
                    )
                    
                    if success:
                        successful_updates += 1
                        
                except Exception as e:
                    self.logger.error("Failed to save price update", symbol=update.get('symbol'), error=str(e))
                    continue
        
        self.logger.info("Price updates saved", total=len(price_updates), successful=successful_updates)
    
    def _update_stats(self, price_updates: list, start_time: datetime):
        """Обновить статистику выполнения"""
        self.stats['duration_seconds'] = (datetime.utcnow() - start_time).total_seconds()
        self.stats['prices_updated'] = len(price_updates)
        
        # Оценка API запросов (4 страницы по 250 токенов)
        self.stats['api_requests_used'] = 4
    
    def get_stats(self) -> dict:
        """Получить статистику последнего выполнения"""
        return self.stats.copy()