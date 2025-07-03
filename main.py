

import asyncio
import signal
import sys
from typing import Optional
import click
from utils.logger import setup_logger
from scheduler.job_manager import JobManager
from database.repositories.token_repo import TokenRepository
from database.repositories.exchange_repo import ExchangeRepository
from config import Config

logger = setup_logger()

class CoinGeckoParserApp:
    def __init__(self):
        self.job_manager: Optional[JobManager] = None
        self.logger = logger.bind(component="App")
        self.shutdown_event = asyncio.Event()
    
    async def startup(self):
        try:
            self.logger.info("Starting CoinGecko Parser")
            
            self.job_manager = JobManager()
            self.job_manager.start_all_jobs()
            
            self.logger.info(
                "Application started successfully",
                top_exchanges=Config.TOP_EXCHANGES_LIMIT,
                top_tokens=Config.TOP_TOKENS_LIMIT
            )
            
        except Exception as e:
            self.logger.error("Failed to start application", error=str(e))
            raise
    
    async def shutdown(self):
        try:
            self.logger.info("Shutting down application")
            
            if self.job_manager:
                self.job_manager.stop_all_jobs()
            
            self.logger.info("Application shutdown complete")
            
        except Exception as e:
            self.logger.error("Error during shutdown", error=str(e))
    
    def setup_signal_handlers(self):
        def signal_handler(signum, frame):
            self.logger.info("Received shutdown signal", signal=signum)
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def run(self):
        try:
            await self.startup()
            await self.shutdown_event.wait()
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error("Application error", error=str(e))
            raise
        finally:
            await self.shutdown()

@click.group()
def cli():
    """CoinGecko Parser CLI"""
    pass

@cli.command()
def start():
    """Запустить парсер"""
    app = CoinGeckoParserApp()
    app.setup_signal_handlers()
    
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error("Application failed", error=str(e))
        sys.exit(1)

@cli.command()
@click.option(
    '--job', 
    type=click.Choice([
        'daily_full_sync',
        'hourly_price_update',
        'health_check',
        'maintenance_cleanup'
    ]), 
    required=True
)
def run_once(job):
    """Запустить задачу один раз"""
    async def run_job():
        job_manager = JobManager()
        await job_manager.run_job_once(job)
    
    try:
        asyncio.run(run_job())
        logger.info("Job completed successfully", job=job)
    except Exception as e:
        logger.error("Job failed", job=job, error=str(e))
        sys.exit(1)

@cli.command()
def status():
    """Показать статус системы"""
    async def check_status():
        job_manager = JobManager()
        stats = job_manager.get_comprehensive_stats()
        
        click.echo("=== System Status ===")
        global_stats = stats['global_stats']
        click.echo(f"Total runs: {global_stats['total_runs']}")
        click.echo(f"Successful runs: {global_stats['successful_runs']}")
        click.echo(f"Failed runs: {global_stats['failed_runs']}")
        click.echo(f"API requests today: {global_stats['api_requests_today']}")
        
        click.echo("\n=== Last Sync Job ===")
        sync_stats = stats['sync_job_stats']
        click.echo(f"Last run: {sync_stats['last_run']}")
        click.echo(f"Duration: {sync_stats['duration_seconds']:.1f}s")
        click.echo(f"Exchanges: {sync_stats['exchanges_processed']}")
        click.echo(f"Tokens: {sync_stats['tokens_processed']}")
        
        click.echo("\n=== Last Update Job ===")
        update_stats = stats['update_job_stats']
        click.echo(f"Last run: {update_stats['last_run']}")
        click.echo(f"Duration: {update_stats['duration_seconds']:.1f}s")
        click.echo(f"Prices updated: {update_stats['prices_updated']}")
        
        click.echo("\n=== Rate Limiting ===")
        rate_stats = stats.get('rate_limit_stats', {})
        if rate_stats:
            click.echo(f"Requests in window: {rate_stats.get('requests_in_window', 0)}")
            click.echo(f"Remaining requests: {rate_stats.get('remaining_requests', 0)}")
        
        click.echo("\n=== Scheduled Jobs ===")
        for job in stats['scheduler_stats']['jobs']:
            click.echo(f"- {job['id']}: next run at {job['next_run_time']}")
    
    try:
        asyncio.run(check_status())
    except Exception as e:
        click.echo(f"Error checking status: {e}")
        sys.exit(1)

@cli.command()
def config():
    """Показать конфигурацию"""
    click.echo("=== Configuration ===")
    click.echo(f"AWS Region: {Config.AWS_REGION}")
    click.echo(f"DynamoDB Table: {Config.DYNAMODB_TABLE}")
    click.echo(f"Top Exchanges Limit: {Config.TOP_EXCHANGES_LIMIT}")
    click.echo(f"Top Tokens Limit: {Config.TOP_TOKENS_LIMIT}")
    click.echo(f"Rate Limit: {Config.RATE_LIMIT_REQUESTS} req/{Config.RATE_LIMIT_WINDOW}s")
    click.echo(f"Log Level: {Config.LOG_LEVEL}")
    click.echo(f"Stats TTL: {Config.TTL_DAYS_STATS} days")
    click.echo(f"Info TTL: {Config.TTL_DAYS_INFO} days")

@cli.command()
@click.option('--symbol', required=True, help='Token symbol')
def get_token(symbol):
    """Получить данные токена"""
    async def fetch_token():
        token_repo = TokenRepository()
        
        click.echo(f"=== Token Data for {symbol.upper()} ===")
        click.echo("Searching in database...")
        
        # В реальном проекте нужен поиск по символу
        # Пока показываем заглушку
        click.echo("Token search functionality will be implemented")
    
    try:
        asyncio.run(fetch_token())
    except Exception as e:
        click.echo(f"Error: {e}")
        sys.exit(1)

@cli.command()
def get_exchanges():
    """Получить данные бирж"""
    async def fetch_exchanges():
        exchange_repo = ExchangeRepository()
        
        click.echo("=== Top Exchanges ===")
        
        try:
            # Получаем топ биржи
            top_exchanges = exchange_repo.get_top_exchanges_by_volume(limit=10)
            
            if top_exchanges:
                for i, exchange in enumerate(top_exchanges, 1):
                    name = exchange.get('name', 'Unknown')
                    volume = exchange.get('trading_volume_24h', 0)
                    click.echo(f"{i}. {name}: ${volume:,.0f} 24h volume")
            else:
                click.echo("No exchange data found. Run sync first.")
                
        except Exception as e:
            click.echo(f"Error getting exchanges: {e}")
    
    try:
        asyncio.run(fetch_exchanges())
    except Exception as e:
        click.echo(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    cli()