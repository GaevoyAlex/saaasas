from datetime import datetime
from typing import List, Dict, Any
from .base import BaseRepository
from ..models.token import Token, TokenStats
from ..models.platform import Platform
from config import Config

class TokenRepository(BaseRepository):
    
    def _token_to_item(self, token: Token) -> Dict[str, Any]:
        return {
            'pk': f"TOKEN#{token.id}",
            'sk': f"INFO#{token.updated_at.isoformat()}",
            'entity_type': 'token',
            'id': token.id,
            'exchanges': token.exchanges,
            'name': token.name,
            'symbol': token.symbol,
            'description': token.description,
            'instagram': token.instagram,
            'discord': token.discord,
            'website': token.website,
            'facebook': token.facebook,
            'reddit': token.reddit,
            'twitter': token.twitter,
            'repositories_link': token.repositories_link,
            'whitepaper_link': token.whitepaper_link,
            'tvl': float(token.tvl),
            'avatar_image': token.avatar_image,
            'security_audits': token.security_audits,
            'related_people': token.related_people,
            'created_at': token.created_at.isoformat(),
            'updated_at': token.updated_at.isoformat(),
            'is_deleted': token.is_deleted,
            'ttl': self._create_ttl(Config.TTL_DAYS_INFO)
        }
    
    def _token_stats_to_item(self, stats: TokenStats) -> Dict[str, Any]:
        return {
            'pk': f"TOKEN#{stats.token_id}",
            'sk': f"STATS#{stats.updated_at.isoformat()}",
            'entity_type': 'token_stats',
            'id': stats.id,
            'token_id': stats.token_id,
            'security_audits': stats.security_audits,
            'market_cap': float(stats.market_cap) if stats.market_cap else None,
            'trading_volume_24h': float(stats.trading_volume_24h) if stats.trading_volume_24h else None,
            'token_max_supply': stats.token_max_supply,
            'token_total_supply': stats.token_total_supply,
            'transactions_count_30d': stats.transactions_count_30d,
            'volume_1m_change_1m': stats.volume_1m_change_1m,
            'volume_24h_change_24h': float(stats.volume_24h_change_24h) if stats.volume_24h_change_24h else None,
            'price': float(stats.price),
            'fdv': float(stats.fdv) if stats.fdv else None,
            'ath': float(stats.ath) if stats.ath else None,
            'atl': float(stats.atl) if stats.atl else None,
            'created_at': stats.created_at.isoformat(),
            'updated_at': stats.updated_at.isoformat(),
            'is_deleted': stats.is_deleted,
            'ttl': self._create_ttl(Config.TTL_DAYS_STATS)
        }
    
    def _platform_to_item(self, platform: Platform) -> Dict[str, Any]:
        return {
            'pk': f"TOKEN#{platform.token_id}",
            'sk': f"PLATFORM#{platform.name}#{platform.updated_at.isoformat()}",
            'entity_type': 'platform',
            'id': platform.id,
            'token_id': platform.token_id,
            'name': platform.name,
            'symbol': platform.symbol,
            'token_address': platform.token_address,
            'created_at': platform.created_at.isoformat(),
            'updated_at': platform.updated_at.isoformat(),
            'is_deleted': platform.is_deleted,
            'ttl': self._create_ttl(Config.TTL_DAYS_INFO)
        }
    
    def save_tokens(self, tokens: List[Token]) -> bool:
        items = [self._token_to_item(token) for token in tokens]
        return self._batch_save(items)
    
    def save_token_stats(self, stats_list: List[TokenStats]) -> bool:
        items = [self._token_stats_to_item(stats) for stats in stats_list]
        return self._batch_save(items)
    
    def save_platforms(self, platforms: List[Platform]) -> bool:
        items = [self._platform_to_item(platform) for platform in platforms]
        return self._batch_save(items)
    
    def save_batch(self, models: List[Any]) -> bool:
        all_items = []
        
        for model in models:
            if isinstance(model, Token):
                all_items.append(self._token_to_item(model))
            elif isinstance(model, TokenStats):
                all_items.append(self._token_stats_to_item(model))
            elif isinstance(model, Platform):
                all_items.append(self._platform_to_item(model))
        
        return self._batch_save(all_items)
    
    def get_latest(self, token_id: str) -> Dict[str, Any]:
        items = self.db_client.query_by_pk(f"TOKEN#{token_id}", limit=10)
        
        result = {
            'token': None,
            'stats': None,
            'platforms': []
        }
        
        for item in items:
            entity_type = item.get('entity_type')
            if entity_type == 'token' and not result['token']:
                result['token'] = item
            elif entity_type == 'token_stats' and not result['stats']:
                result['stats'] = item
            elif entity_type == 'platform':
                result['platforms'].append(item)
        
        return result
    
    def get_token_by_symbol(self, symbol: str) -> Dict[str, Any]:
 
        return {}
    
    def quick_price_update(self, symbol: str, price: float, market_cap: float = None) -> bool:
        """Быстрое обновление цены токена"""
        try:
            item = {
                'pk': f"QUICK_PRICE#{symbol.upper()}",
                'sk': f"PRICE#{datetime.utcnow().isoformat()}",
                'entity_type': 'quick_price',
                'symbol': symbol.upper(),
                'price': price,
                'market_cap': market_cap,
                'updated_at': datetime.utcnow().isoformat(),
                'ttl': self._create_ttl(1)  # 1 день для быстрых обновлений
            }
            
            return self.db_client.put_item(item)
            
        except Exception as e:
            self.logger.error("Error in quick price update", symbol=symbol, error=str(e))
            return False