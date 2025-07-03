from typing import List, Dict, Any
from .base import BaseRepository
from ..models.exchange import Exchange, ExchangeStats
from config import Config

class ExchangeRepository(BaseRepository):
    
    def _exchange_to_item(self, exchange: Exchange) -> Dict[str, Any]:
        return {
            'pk': f"EXCHANGE#{exchange.id}",
            'sk': f"INFO#{exchange.updated_at.isoformat()}",
            'entity_type': 'exchange',
            'id': exchange.id,
            'name': exchange.name,
            'description': exchange.description,
            'islamic_account': exchange.islamic_account,
            'facebook': exchange.facebook,
            'youtube': exchange.youtube,
            'instagram': exchange.instagram,
            'medium': exchange.medium,
            'discord': exchange.discord,
            'website': exchange.website,
            'twitter': exchange.twitter,
            'reddit': exchange.reddit,
            'repositories_link': exchange.repositories_link,
            'avatar_image': exchange.avatar_image,
            'native_token_symbol': exchange.native_token_symbol,
            'security_audits': exchange.security_audits,
            'related_people': exchange.related_people,
            'trading_pairs': exchange.trading_pairs,
            'created_at': exchange.created_at.isoformat(),
            'updated_at': exchange.updated_at.isoformat(),
            'is_deleted': exchange.is_deleted,
            'ttl': self._create_ttl(Config.TTL_DAYS_INFO)
        }
    
    def _exchange_stats_to_item(self, stats: ExchangeStats) -> Dict[str, Any]:
        return {
            'pk': f"EXCHANGE#{stats.exchange_id}",
            'sk': f"STATS#{stats.updated_at.isoformat()}",
            'entity_type': 'exchange_stats',
            'id': stats.id,
            'exchange_id': stats.exchange_id,
            'inflows_1m': float(stats.inflows_1m) if stats.inflows_1m else None,
            'inflows_1w': float(stats.inflows_1w) if stats.inflows_1w else None,
            'inflows_24h': float(stats.inflows_24h) if stats.inflows_24h else None,
            'trading_volume_1m': float(stats.trading_volume_1m) if stats.trading_volume_1m else None,
            'trading_volume_1w': float(stats.trading_volume_1w) if stats.trading_volume_1w else None,
            'trading_volume_24h': float(stats.trading_volume_24h) if stats.trading_volume_24h else None,
            'visitors_7d': stats.visitors_7d,
            'reserves': float(stats.reserves) if stats.reserves else None,
            'fiat_supported': stats.fiat_supported,
            'coins_count': stats.coins_count,
            'liquidity_score': float(stats.liquidity_score),
            'effective_liquidity_24h': float(stats.effective_liquidity_24h),
            'percent_change_volume_24h': float(stats.percent_change_volume_24h),
            'percent_change_volume_1m': float(stats.percent_change_volume_1m),
            'percent_change_volume_7d': float(stats.percent_change_volume_7d),
            'created_at': stats.created_at.isoformat(),
            'updated_at': stats.updated_at.isoformat(),
            'is_deleted': stats.is_deleted,
            'ttl': self._create_ttl(Config.TTL_DAYS_STATS)
        }
    
    def save_exchanges(self, exchanges: List[Exchange]) -> bool:
        items = [self._exchange_to_item(exchange) for exchange in exchanges]
        return self._batch_save(items)
    
    def save_exchange_stats(self, stats_list: List[ExchangeStats]) -> bool:
        items = [self._exchange_stats_to_item(stats) for stats in stats_list]
        return self._batch_save(items)
    
    def save_batch(self, models: List[Any]) -> bool:
        all_items = []
        
        for model in models:
            if isinstance(model, Exchange):
                all_items.append(self._exchange_to_item(model))
            elif isinstance(model, ExchangeStats):
                all_items.append(self._exchange_stats_to_item(model))
        
        return self._batch_save(all_items)
    
    def get_latest(self, exchange_id: str) -> Dict[str, Any]:
        items = self.db_client.query_by_pk(f"EXCHANGE#{exchange_id}", limit=5)
        
        result = {
            'exchange': None,
            'stats': None
        }
        
        for item in items:
            entity_type = item.get('entity_type')
            if entity_type == 'exchange' and not result['exchange']:
                result['exchange'] = item
            elif entity_type == 'exchange_stats' and not result['stats']:
                result['stats'] = item
        
        return result
    
    def get_top_exchanges_by_volume(self, limit: int = 20) -> List[Dict[str, Any]]:
 
        exchanges_stats = []
        
 
        try:
 
            pass
        except Exception as e:
            self.logger.error("Error getting top exchanges", error=str(e))
        
        return exchanges_stats[:limit]