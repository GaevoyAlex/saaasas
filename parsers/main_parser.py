import asyncio
from typing import Dict, List, Any, Tuple
import structlog
from .client.api_client import CoinGeckoAPIClient
from .processors.exchange_processor import ExchangeProcessor
from .processors.token_processor import TokenProcessor
from database.models.exchange import Exchange, ExchangeStats
from database.models.token import Token, TokenStats
from database.models.platform import Platform
from config import Config

logger = structlog.get_logger()

class MainCoinGeckoParser:
    def __init__(self):
        self.api_client = CoinGeckoAPIClient()
        self.exchange_processor = ExchangeProcessor()
        self.token_processor = TokenProcessor()
        self.logger = logger.bind(component="MainParser")
    
    async def parse_full_data(self) -> Dict[str, List]:
        """Полный парсинг всех данных (используется раз в день)"""
        self.logger.info("Starting full data parsing")
        
        result = {
            'exchanges': [],
            'exchange_stats': [],
            'tokens': [],
            'token_stats': [],
            'platforms': []
        }
        
        try:
            # 1. Получаем базовые списки
            exchanges_list, coins_list = await asyncio.gather(
                self.api_client.get_exchanges_list(),
                self.api_client.get_coins_list(include_platform=True)
            )
            
            # 2. Парсим биржи (топ-N)
            if exchanges_list:
                exchanges, exchange_stats = await self._parse_exchanges(
                    exchanges_list[:Config.TOP_EXCHANGES_LIMIT]
                )
                result['exchanges'].extend(exchanges)
                result['exchange_stats'].extend(exchange_stats)
            
            # 3. Парсим токены через markets API
            if coins_list:
                tokens, token_stats, platforms = await self._parse_tokens_from_markets(
                    coins_list[:Config.TOP_TOKENS_LIMIT]
                )
                result['tokens'].extend(tokens)
                result['token_stats'].extend(token_stats)
                result['platforms'].extend(platforms)
            
            # 4. Дополняем детальными данными для топ токенов
            top_tokens_detailed = await self._parse_top_tokens_detailed()
            if top_tokens_detailed:
                result['tokens'].extend(top_tokens_detailed['tokens'])
                result['token_stats'].extend(top_tokens_detailed['token_stats'])
                result['platforms'].extend(top_tokens_detailed['platforms'])
            
            total_items = sum(len(v) for v in result.values())
            self.logger.info("Full data parsing completed", total_items=total_items)
            
            return result
            
        except Exception as e:
            self.logger.error("Full data parsing failed", error=str(e))
            return result
    
    async def parse_quick_prices(self) -> List[Dict[str, Any]]:
        """Быстрое обновление цен (используется каждый час)"""
        self.logger.info("Starting quick price parsing")
        
        price_updates = []
        
        try:
            # Получаем рыночные данные по страницам
            for page in range(1, 5):  # Топ-1000 токенов 
                market_data = await self.api_client.get_coins_markets(
                    vs_currency='usd',
                    page=page,
                    per_page=250
                )
                
                if not market_data:
                    break
                
                # Создаем быстрые обновления цен
                for coin in market_data:
                    price_update = self.token_processor.create_quick_price_update(
                        symbol=coin.get('symbol', ''),
                        price=coin.get('current_price', 0),
                        market_cap=coin.get('market_cap', 0)
                    )
                    price_updates.append(price_update)
            
            self.logger.info("Quick price parsing completed", updates_count=len(price_updates))
            return price_updates
            
        except Exception as e:
            self.logger.error("Quick price parsing failed", error=str(e))
            return price_updates
    
    async def _parse_exchanges(self, exchanges_list: List[Dict[str, Any]]) -> Tuple[List[Exchange], List[ExchangeStats]]:
         
        exchanges = []
        exchange_stats = []
        
        self.logger.info("Parsing exchanges", count=len(exchanges_list))
        
        
        batch_size = 10
        for i in range(0, len(exchanges_list), batch_size):
            batch = exchanges_list[i:i + batch_size]
            
             
            detailed_data_tasks = []
            for exchange_info in batch:
                exchange_id = exchange_info.get('id')
                if exchange_id:
                    detailed_data_tasks.append(
                        self.api_client.get_exchange_details(exchange_id)
                    )
            
             
            detailed_data_list = await asyncio.gather(*detailed_data_tasks, return_exceptions=True)
            
             
            for j, detailed_data in enumerate(detailed_data_list):
                if isinstance(detailed_data, Exception):
                    self.logger.error("Failed to get exchange details", error=str(detailed_data))
                    continue
                
                try:
                    exchange, stats = self.exchange_processor.process_exchange_data(detailed_data)
                    exchanges.append(exchange)
                    exchange_stats.append(stats)
                except Exception as e:
                    self.logger.error("Failed to process exchange", error=str(e))
                    continue
        
        return exchanges, exchange_stats
    
    async def _parse_tokens_from_markets(self, coins_list: List[Dict[str, Any]]) -> Tuple[List[Token], List[TokenStats], List[Platform]]:
         
        tokens = []
        token_stats = []
        platforms = []
        
        self.logger.info("Parsing tokens from markets", count=len(coins_list))
        
        
        coins_map = {coin['id']: coin for coin in coins_list}
        
         
        for page in range(1, 5):   
            try:
                market_data = await self.api_client.get_coins_markets(
                    vs_currency='usd',
                    page=page,
                    per_page=250
                )
                
                if not market_data:
                    break
                
                 
                for coin_market in market_data:
                    try:
                        
                        token, stats, token_platforms = self.token_processor.process_market_token_data(coin_market)
                        
                        
                        coin_id = coin_market.get('id')
                        if coin_id in coins_map:
                            _, additional_platforms = self.token_processor.update_token_with_coins_list_data(
                                token, coins_map[coin_id]
                            )
                            token_platforms.extend(additional_platforms)
                        
                        tokens.append(token)
                        token_stats.append(stats)
                        platforms.extend(token_platforms)
                        
                    except Exception as e:
                        self.logger.error("Failed to process market token", error=str(e))
                        continue
                        
            except Exception as e:
                self.logger.error("Failed to get market data page", page=page, error=str(e))
                break
        
        return tokens, token_stats, platforms
    
    async def _parse_top_tokens_detailed(self, limit: int = 20) -> Dict[str, List]:
         
        result = {
            'tokens': [],
            'token_stats': [],
            'platforms': []
        }
        
        try:
           
            top_market_data = await self.api_client.get_coins_markets(
                vs_currency='usd',
                page=1,
                per_page=limit
            )
            
            if not top_market_data:
                return result
            
            top_coin_ids = [coin['id'] for coin in top_market_data if coin.get('id')]
            
            self.logger.info("Parsing detailed data for top tokens", count=len(top_coin_ids))
            
            
            for coin_id in top_coin_ids:
                try:
                    detailed_data = await self.api_client.get_coin_details(coin_id)
                    
                    if detailed_data:
                        token, stats, platforms = self.token_processor.process_detailed_token_data(detailed_data)
                        result['tokens'].append(token)
                        result['token_stats'].append(stats)
                        result['platforms'].extend(platforms)
                        
                except Exception as e:
                    self.logger.error("Failed to get detailed token data", coin_id=coin_id, error=str(e))
                    continue
            
        except Exception as e:
            self.logger.error("Failed to parse top tokens detailed", error=str(e))
        
        return result
    
    async def validate_api_connection(self) -> bool:
         return await self.api_client.validate_connection()
    
    def get_rate_limit_stats(self) -> Dict[str, Any]:
         return self.api_client.get_rate_limit_stats()