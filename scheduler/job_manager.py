import asyncio
from datetime import datetime
import structlog
from .base_scheduler import BaseScheduler
from .jobs.sync_job import FullSyncJob
from .jobs.update_job import QuickUpdateJob
from parsers.main_parser import MainCoinGeckoParser
from database.repositories.token_repo import TokenRepository

logger = structlog.get_logger()

class JobManager:
    def __init__(self):
        self.scheduler = BaseScheduler()
        self.sync_job = FullSyncJob()
        self.update_job = QuickUpdateJob()
        self.parser = MainCoinGeckoParser()
        self.token_repo = TokenRepository()
        self.logger = logger.bind(component="JobManager")
        
        self.global_stats = {
            'total_runs': 0,
            'successful_runs': 0,
            'failed_runs': 0,
            'last_error': None,
            'api_requests_today': 0
        }
    
    async def daily_full_sync(self):
        """Ежедневная полная синхронизация"""
        self.global_stats['total_runs'] += 1
        
        try:
            await self.sync_job.execute()
            self.global_stats['successful_runs'] += 1
            
            # Обновляем счетчик API запросов
            sync_stats = self.sync_job.get_stats()
            self.global_stats['api_requests_today'] += sync_stats.get('api_requests_used', 0)
            
        except Exception as e:
            self.global_stats['failed_runs'] += 1
            self.global_stats['last_error'] = str(e)
            self.logger.error("Daily sync failed", error=str(e))
            raise
    
    async def hourly_price_update(self):
        """Почасовое обновление цен"""
        try:
            await self.update_job.execute()
            
            # Обновляем счетчик API запросов
            update_stats = self.update_job.get_stats()
            self.global_stats['api_requests_today'] += update_stats.get('api_requests_used', 0)
            
        except Exception as e:
            self.logger.error("Hourly update failed", error=str(e))
    
    async def health_check(self):
        """Проверка состояния системы"""
        try:
            self.logger.info("Running health check")
            
            health_status = {
                'timestamp': datetime.utcnow().isoformat(),
                'api_connection': False,
                'database_connection': False,
                'rate_limit_stats': {},
                'global_stats': self.global_stats
            }
            
            # Проверка API
            health_status['api_connection'] = await self.parser.validate_api_connection()
            health_status['rate_limit_stats'] = self.parser.get_rate_limit_stats()
            
            # Проверка БД (попытка получить данные)
            try:
                test_data = self.token_repo.get_latest('test-token-id')
                health_status['database_connection'] = True
            except Exception:
                health_status['database_connection'] = False
            
            self.logger.info("Health check completed", status=health_status)
            
        except Exception as e:
            self.logger.error("Health check failed", error=str(e))
    
    async def maintenance_cleanup(self):
        """Техническое обслуживание и очистка"""
        try:
            self.logger.info("Running maintenance cleanup")
            
            # Сброс дневного счетчика API запросов
            self.global_stats['api_requests_today'] = 0
            
            # Здесь можно добавить другие задачи обслуживания
            # например, анализ производительности, очистка логов и т.д.
            
            self.logger.info("Maintenance cleanup completed")
            
        except Exception as e:
            self.logger.error("Maintenance cleanup failed", error=str(e))
    
    def start_all_jobs(self):
        """Запустить все задачи по расписанию"""
        try:
            # Ежедневная полная синхронизация в 3:00
            self.scheduler.add_cron_job(
                func=self.daily_full_sync,
                cron_expression='0 3 * * *',
                job_id='daily_full_sync'
            )
            
            # Почасовое обновление цен
            self.scheduler.add_interval_job(
                func=self.hourly_price_update,
                seconds=3600,  # каждый час
                job_id='hourly_price_update'
            )
            
            # Проверка состояния каждые 30 минут
            self.scheduler.add_interval_job(
                func=self.health_check,
                seconds=1800,  # 30 минут
                job_id='health_check'
            )
            
            # Техническое обслуживание каждый день в 2:00
            self.scheduler.add_cron_job(
                func=self.maintenance_cleanup,
                cron_expression='0 2 * * *',
                job_id='maintenance_cleanup'
            )
            
            # Запуск планировщика
            self.scheduler.start()
            
            self.logger.info("All jobs started successfully")
            
        except Exception as e:
            self.logger.error("Error starting jobs", error=str(e))
            raise
    
    def stop_all_jobs(self):
        """Остановить все задачи"""
        try:
            self.scheduler.shutdown(wait=True)
            self.logger.info("All jobs stopped")
        except Exception as e:
            self.logger.error("Error stopping jobs", error=str(e))
    
    def get_comprehensive_stats(self) -> dict:
        """Получить полную статистику системы"""
        return {
            'global_stats': self.global_stats,
            'sync_job_stats': self.sync_job.get_stats(),
            'update_job_stats': self.update_job.get_stats(),
            'scheduler_stats': {
                'running': self.scheduler.is_running,
                'jobs': self.scheduler.get_jobs()
            },
            'rate_limit_stats': self.parser.get_rate_limit_stats() if hasattr(self.parser, 'get_rate_limit_stats') else {}
        }
    
    async def run_job_once(self, job_name: str):
        """Запустить конкретную задачу один раз"""
        job_map = {
            'daily_full_sync': self.daily_full_sync,
            'hourly_price_update': self.hourly_price_update,
            'health_check': self.health_check,
            'maintenance_cleanup': self.maintenance_cleanup
        }
        
        if job_name not in job_map:
            raise ValueError(f"Unknown job: {job_name}")
        
        try:
            await job_map[job_name]()
            self.logger.info("Job executed successfully", job_name=job_name)
        except Exception as e:
            self.logger.error("Job execution failed", job_name=job_name, error=str(e))
            raise