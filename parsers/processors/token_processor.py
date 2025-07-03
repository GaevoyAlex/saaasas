import uuid
from typing import List, Dict, Any, Tuple, Optional
import structlog
from database.models.token import Token, TokenStats
from database.models.platform import Platform
from .data_calculator import DataCalculator
from datetime import datetime

logger = structlog.get_logger()

class TokenProcessor:
    def __init__(self):
        self.calculator = DataCalculator()
        self.logger = logger.bind(component="TokenProcessor")
    
    def process_market_token_data(self, market_data: Dict[str, Any]) -> Tuple[Token, TokenStats, List[Platform]]:
        """Обработать данные токена из markets API"""
        try:
            token_id = str(uuid.uuid4())
            
            # Создаем основную запись токена
            token = Token(
                id=token_id,
                name=market_data.get('name', ''),
                symbol=market_data.get('symbol', '').upper(),
                avatar_image=market_data.get('image', '')
            )
            
            # Создаем статистику токена
            current_price = market_data.get('current_price', 0)
            market_cap = market_data.get('market_cap', 0)
            total_volume = market_data.get('total_volume', 0)
            circulating_supply = market_data.get('circulating_supply', 0)
            total_supply = market_data.get('total_supply', 0)
            max_supply = market_data.get('max_supply', 0)
            
            # Рассчитываем FDV
            fdv = self.calculator.calculate_fdv(current_price, max_supply)
            
            # Оценка транзакций за 30 дней
            transactions_30d = self.calculator.estimate_transactions_count_30d(
                total_volume, current_price
            )
            
            token_stats = TokenStats(
                id=str(uuid.uuid4()),
                token_id=token_id,
                market_cap=self.calculator.safe_decimal_conversion(market_cap),
                trading_volume_24h=self.calculator.safe_decimal_conversion(total_volume),
                token_max_supply=int(max_supply) if max_supply else None,
                token_total_supply=int(total_supply) if total_supply else None,
                transactions_count_30d=transactions_30d,
                volume_24h_change_24h=self.calculator.safe_decimal_conversion(
                    market_data.get('price_change_percentage_24h', 0)
                ),
                price=self.calculator.safe_decimal_conversion(current_price),
                fdv=fdv,
                ath=self.calculator.safe_decimal_conversion(market_data.get('ath', 0)),
                atl=self.calculator.safe_decimal_conversion(market_data.get('atl', 0))
            )
            
            # Создаем платформы (пока пустой список, заполним отдельно)
            platforms = []
            
            self.logger.info("Market token processed", symbol=token.symbol, price=current_price)
            
            return token, token_stats, platforms
            
        except Exception as e:
            self.logger.error("Error processing market token", error=str(e), symbol=market_data.get('symbol'))
            raise
    
    def process_detailed_token_data(self, detailed_data: Dict[str, Any]) -> Tuple[Token, TokenStats, List[Platform]]:
        """Обработать детальные данные токена"""
        try:
            token_id = str(uuid.uuid4())
            
            # Создаем основную запись токена с детальной информацией
            links = self.calculator.normalize_social_links(detailed_data.get('links', {}))
            
            token = Token(
                id=token_id,
                name=detailed_data.get('name', ''),
                symbol=detailed_data.get('symbol', '').upper(),
                description=detailed_data.get('description', {}).get('en', '')[:500],  # Ограничиваем длину
                website=links['website'],
                twitter=links['twitter'],
                facebook=links['facebook'],
                reddit=links['reddit'],
                repositories_link=links['repositories_link'],
                whitepaper_link=links['whitepaper_link'],
                avatar_image=detailed_data.get('image', {}).get('large', ''),
                tvl=self.calculator.safe_decimal_conversion(0)  # TVL отдельно
            )
            
            # Обрабатываем рыночные данные
            market_data = detailed_data.get('market_data', {})
            current_price_usd = market_data.get('current_price', {}).get('usd', 0)
            market_cap_usd = market_data.get('market_cap', {}).get('usd', 0)
            total_volume_usd = market_data.get('total_volume', {}).get('usd', 0)
            
            # Рассчитываем FDV
            max_supply = market_data.get('max_supply')
            fdv = self.calculator.calculate_fdv(current_price_usd, max_supply)
            
            # Оценка транзакций
            transactions_30d = self.calculator.estimate_transactions_count_30d(
                total_volume_usd, current_price_usd
            )
            
            token_stats = TokenStats(
                id=str(uuid.uuid4()),
                token_id=token_id,
                market_cap=self.calculator.safe_decimal_conversion(market_cap_usd),
                trading_volume_24h=self.calculator.safe_decimal_conversion(total_volume_usd),
                token_max_supply=int(max_supply) if max_supply else None,
                token_total_supply=int(market_data.get('total_supply')) if market_data.get('total_supply') else None,
                transactions_count_30d=transactions_30d,
                volume_24h_change_24h=self.calculator.safe_decimal_conversion(
                    market_data.get('price_change_percentage_24h', 0)
                ),
                price=self.calculator.safe_decimal_conversion(current_price_usd),
                fdv=fdv,
                ath=self.calculator.safe_decimal_conversion(market_data.get('ath', {}).get('usd', 0)),
                atl=self.calculator.safe_decimal_conversion(market_data.get('atl', {}).get('usd', 0))
            )
            
            # Создаем платформы
            platforms = self._create_platforms_from_detailed_data(token_id, detailed_data)
            
            self.logger.info("Detailed token processed", symbol=token.symbol, platforms_count=len(platforms))
            
            return token, token_stats, platforms
            
        except Exception as e:
            self.logger.error("Error processing detailed token", error=str(e), coin_id=detailed_data.get('id'))
            raise
    
    def _create_platforms_from_detailed_data(self, token_id: str, detailed_data: Dict[str, Any]) -> List[Platform]:
        """Создать платформы из детальных данных"""
        platforms = []
        
        try:
            platforms_data = detailed_data.get('platforms', {})
            symbol = detailed_data.get('symbol', '').upper()
            
            for platform_name, contract_address in platforms_data.items():
                if contract_address and contract_address.strip():
                    platform = Platform(
                        id=str(uuid.uuid4()),
                        token_id=token_id,
                        name=platform_name,
                        symbol=symbol,
                        token_address=contract_address.strip()
                    )
                    platforms.append(platform)
            
        except Exception as e:
            self.logger.error("Error creating platforms", error=str(e))
        
        return platforms
    
    def process_tokens_batch(self, tokens_data: List[Dict[str, Any]], is_detailed: bool = False) -> Tuple[List[Token], List[TokenStats], List[Platform]]:
        """Обработать батч токенов"""
        tokens = []
        tokens_stats = []
        all_platforms = []
        
        for token_data in tokens_data:
            try:
                if is_detailed:
                    token, stats, platforms = self.process_detailed_token_data(token_data)
                else:
                    token, stats, platforms = self.process_market_token_data(token_data)
                
                tokens.append(token)
                tokens_stats.append(stats)
                all_platforms.extend(platforms)
                
            except Exception as e:
                self.logger.error("Failed to process token in batch", error=str(e))
                continue
        
        self.logger.info(
            "Tokens batch processed", 
            total=len(tokens_data), 
            successful=len(tokens),
            platforms=len(all_platforms)
        )
        
        return tokens, tokens_stats, all_platforms
    
    def update_token_with_coins_list_data(self, token: Token, coins_list_data: Dict[str, Any]) -> Tuple[Token, List[Platform]]:
        """Обновить токен данными из coins/list API"""
        try:
            platforms = []
            
            # Создаем платформы из coins/list данных
            platforms_data = coins_list_data.get('platforms', {})
            
            for platform_name, contract_address in platforms_data.items():
                if contract_address and contract_address.strip():
                    platform = Platform(
                        id=str(uuid.uuid4()),
                        token_id=token.id,
                        name=platform_name,
                        symbol=token.symbol,
                        token_address=contract_address.strip()
                    )
                    platforms.append(platform)
            
            return token, platforms
            
        except Exception as e:
            self.logger.error("Error updating token with coins list data", error=str(e))
            return token, []
    
    def calculate_volume_changes(self, token_stats: TokenStats, historical_data: Dict[str, float]) -> TokenStats:
        """Рассчитать изменения объемов на основе исторических данных"""
        try:
            current_volume = float(token_stats.trading_volume_24h or 0)
            
            # Рассчитываем изменение за месяц
            volume_change_1m = self.calculator.calculate_volume_change_1m(
                current_volume, historical_data
            )
            
            if volume_change_1m is not None:
                token_stats.volume_1m_change_1m = volume_change_1m
            
            return token_stats
            
        except Exception as e:
            self.logger.error("Error calculating volume changes", error=str(e))
            return token_stats
    
    def enhance_token_with_community_data(self, token: Token, community_data: Dict[str, Any]) -> Token:
        """Дополнить токен данными сообщества"""
        try:
            # Если нет социальных ссылок, можем попробовать их восстановить
            if not token.twitter and community_data.get('twitter_followers', 0) > 0:
                # Можно попытаться найти Twitter по названию токена
                pass
            
            if not token.reddit and community_data.get('reddit_subscribers', 0) > 0:
                # Аналогично для Reddit
                pass
            
            return token
            
        except Exception as e:
            self.logger.error("Error enhancing token with community data", error=str(e))
            return token
    
    def create_quick_price_update(self, symbol: str, price: float, market_cap: float = None) -> Dict[str, Any]:
        """Создать быстрое обновление цены"""
        return {
            'symbol': symbol.upper(),
            'price': price,
            'market_cap': market_cap,
            'updated_at': datetime.utcnow()
        }