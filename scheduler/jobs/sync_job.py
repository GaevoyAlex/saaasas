import asyncio
from datetime import datetime
import structlog
from parsers.main_parser import MainCoinGeckoParser
from database.repositories.exchange_repo import ExchangeRepository
from database.repositories.token_repo import TokenRepository

logger = structlog.get_logger()

class FullSyncJob:
    def __init__(self):
        self.parser = MainCoinGeckoParser()
        self.exchange_repo = ExchangeRepository()
        self.token_repo = TokenRepository()
        self.logger = logger.bind(component="FullSyncJob")
        
        self.stats = {
            'last_run': None,
            'duration_seconds': 0,
            'exchanges_processed': 0,
            'tokens_processed': 0,
            'platforms_processed': 0,
            'api_requests_used': 0
        }
    
    async def execute(self):
        """Выполнить полную синхронизацию"""
        start_time = datetime.utcnow()
        self.stats['last_run'] = start_time.isoformat()
        
        try:
            self.logger.info("Starting full sync job")
            
            # Проверяем подключение к API
            if not await self.parser.validate_api_connection():
                raise Exception("Cannot connect to CoinGecko API")
            
            # Парсим все данные
            all_data = await self.parser.parse_full_data()
            
            if not any(all_data.values()):
                self.logger.warning("No data received during full sync")
                return
            
            # Сохраняем данные
            await self._save_all_data(all_data)
            
            # Обновляем статистику
            self._update_stats(all_data, start_time)
            
            self.logger.info(
                "Full sync job completed successfully",
                duration_seconds=self.stats['duration_seconds'],
                exchanges=self.stats['exchanges_processed'],
                tokens=self.stats['tokens_processed']
            )
            
        except Exception as e:
            self.logger.error("Full sync job failed", error=str(e))
            self.stats['duration_seconds'] = (datetime.utcnow() - start_time).total_seconds()
            raise
    
    async def _save_all_data(self, all_data: dict):
        """Сохранить все данные в базу"""
        save_tasks = []
        
        # Сохранение бирж
        if all_data.get('exchanges') or all_data.get('exchange_stats'):
            exchanges = all_data.get('exchanges', [])
            exchange_stats = all_data.get('exchange_stats', [])
            all_exchange_models = exchanges + exchange_stats
            
            if all_exchange_models:
                save_tasks.append(self.exchange_repo.save_batch(all_exchange_models))
        
        # Сохранение токенов
        if any([all_data.get('tokens'), all_data.get('token_stats'), all_data.get('platforms')]):
            tokens = all_data.get('tokens', [])
            token_stats = all_data.get('token_stats', [])
            platforms = all_data.get('platforms', [])
            all_token_models = tokens + token_stats + platforms
            
            if all_token_models:
                save_tasks.append(self.token_repo.save_batch(all_token_models))
        
        # Выполняем сохранение параллельно
        if save_tasks:
            results = await asyncio.gather(*save_tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.error("Failed to save data batch", batch_index=i, error=str(result))
                elif not result:
                    self.logger.warning("Data batch save returned False", batch_index=i)
    
    def _update_stats(self, all_data: dict, start_time: datetime):
        """Обновить статистику выполнения"""
        self.stats['duration_seconds'] = (datetime.utcnow() - start_time).total_seconds()
        self.stats['exchanges_processed'] = len(all_data.get('exchanges', []))
        self.stats['tokens_processed'] = len(all_data.get('tokens', []))
        self.stats['platforms_processed'] = len(all_data.get('platforms', []))
        
        # Оценка использованных API запросов
        # Примерная формула: биржи + токены/250 + детальные данные
        estimated_requests = (
            self.stats['exchanges_processed'] +  # 1 запрос на биржу
            max(1, self.stats['tokens_processed'] // 250) +  # markets API
            2  # базовые списки
        )
        self.stats['api_requests_used'] = estimated_requests
    
    def get_stats(self) -> dict:
        """Получить статистику последнего выполнения"""
        return self.stats.copy()