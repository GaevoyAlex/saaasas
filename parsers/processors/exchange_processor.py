import uuid
from typing import List, Dict, Any, Tuple, Optional
import structlog
from database.models.exchange import Exchange, ExchangeStats
from .data_calculator import DataCalculator
from datetime import datetime

logger = structlog.get_logger()

class ExchangeProcessor:
    def __init__(self):
        self.calculator = DataCalculator()
        self.logger = logger.bind(component="ExchangeProcessor")
    
    def process_exchange_data(self, exchange_data: Dict[str, Any]) -> Tuple[Exchange, ExchangeStats]:
 
        try:
            exchange_id = str(uuid.uuid4())
            
 
            exchange = Exchange(
                id=exchange_id,
                name=exchange_data.get('name', ''),
                description=exchange_data.get('description', ''),
                website=exchange_data.get('url', ''),
                twitter=exchange_data.get('twitter_handle', ''),
                facebook=exchange_data.get('facebook_url', ''),
                reddit=bool(exchange_data.get('reddit_url')),
                avatar_image=exchange_data.get('image', ''),
                trading_pairs=[]  # Заполним отдельно если нужно
            )
            
            # Создаем статистику биржи
            trade_volume_24h_btc = exchange_data.get('trade_volume_24h_btc', 0)
            trust_score = exchange_data.get('trust_score', 0)
            
            # Рассчитываем эффективную ликвидность
            effective_liquidity = self.calculator.calculate_effective_liquidity_24h(
                trade_volume_24h_btc or 0
            )
            
            # Рассчитываем счет ликвидности
            liquidity_score = self.calculator.calculate_liquidity_score(
                trade_volume_24h_btc or 0,
                trade_volume_24h_btc or 1,  # Используем объем как приблизительную оценку
                1.0  # 1% спред
            )
            
            exchange_stats = ExchangeStats(
                id=str(uuid.uuid4()),
                exchange_id=exchange_id,
                trading_volume_24h=self.calculator.safe_decimal_conversion(trade_volume_24h_btc),
                liquidity_score=self.calculator.safe_decimal_conversion(trust_score),
                effective_liquidity_24h=effective_liquidity,
                coins_count=0,  # Рассчитаем отдельно
                percent_change_volume_24h=self.calculator.safe_decimal_conversion(0),  # Нужны исторические данные
                percent_change_volume_1m=self.calculator.safe_decimal_conversion(0),   # Нужны исторические данные
                percent_change_volume_7d=self.calculator.safe_decimal_conversion(0)    # Нужны исторические данные
            )
            
            self.logger.info("Exchange processed", name=exchange.name, volume_24h=trade_volume_24h_btc)
            
            return exchange, exchange_stats
            
        except Exception as e:
            self.logger.error("Error processing exchange", error=str(e), exchange_name=exchange_data.get('name'))
            raise
    
    def process_exchanges_batch(self, exchanges_data: List[Dict[str, Any]]) -> Tuple[List[Exchange], List[ExchangeStats]]:

        exchanges = []
        exchanges_stats = []
        
        for exchange_data in exchanges_data:
            try:
                exchange, stats = self.process_exchange_data(exchange_data)
                exchanges.append(exchange)
                exchanges_stats.append(stats)
            except Exception as e:
                self.logger.error("Failed to process exchange in batch", error=str(e))
                continue
        
        self.logger.info("Exchanges batch processed", total=len(exchanges_data), successful=len(exchanges))
        
        return exchanges, exchanges_stats
    
    def update_volume_changes(self, exchange_stats: ExchangeStats, historical_data: Dict[str, float]) -> ExchangeStats:
 
        try:
            current_volume = float(exchange_stats.trading_volume_24h or 0)
            
 
            if 'volume_24h_ago' in historical_data:
                volume_24h_ago = historical_data['volume_24h_ago']
                exchange_stats.percent_change_volume_24h = self.calculator.calculate_percent_change(
                    current_volume, volume_24h_ago
                )
            
            if 'volume_7d_ago' in historical_data:
                volume_7d_ago = historical_data['volume_7d_ago']
                exchange_stats.percent_change_volume_7d = self.calculator.calculate_percent_change(
                    current_volume, volume_7d_ago
                )
            
            if 'volume_30d_ago' in historical_data:
                volume_30d_ago = historical_data['volume_30d_ago']
                exchange_stats.percent_change_volume_1m = self.calculator.calculate_percent_change(
                    current_volume, volume_30d_ago
                )
            
            return exchange_stats
            
        except Exception as e:
            self.logger.error("Error updating volume changes", error=str(e))
            return exchange_stats
    
    def calculate_coins_count(self, exchange_id: str, tickers_data: List[Dict[str, Any]]) -> int:
 
        return self.calculator.calculate_coins_count_for_exchange(tickers_data)
    
    def enhance_exchange_with_social_data(self, exchange: Exchange, detailed_data: Dict[str, Any]) -> Exchange:
 
        try:
 
            if detailed_data.get('facebook_url'):
                exchange.facebook = detailed_data['facebook_url']
            
            if detailed_data.get('reddit_url'):
                exchange.reddit = True
            
            if detailed_data.get('twitter_handle'):
                exchange.twitter = detailed_data['twitter_handle']
             
            if detailed_data.get('country'):
 
                exchange.description += f" | Country: {detailed_data['country']}"
            
            if detailed_data.get('year_established'):
                exchange.description += f" | Founded: {detailed_data['year_established']}"
            
            return exchange
            
        except Exception as e:
            self.logger.error("Error enhancing exchange with social data", error=str(e))
            return exchange