from decimal import Decimal
from typing import Optional, Dict, Any
import structlog

logger = structlog.get_logger()

class DataCalculator:
    """Класс для расчета недостающих в API полей"""
    
    @staticmethod
    def calculate_effective_liquidity_24h(volume_24h: float, spread_percentage: float = 1.0) -> Decimal:
        """Рассчитать эффективную ликвидность за 24 часа"""
        if volume_24h <= 0:
            return Decimal('0')
        
        # Эффективная ликвидность = объем * (1 - спред)
        spread_decimal = Decimal(str(spread_percentage / 100))
        volume_decimal = Decimal(str(volume_24h))
        
        return volume_decimal * (Decimal('1') - spread_decimal)
    
    @staticmethod
    def calculate_percent_change(current: float, previous: float) -> Decimal:
        """Рассчитать процентное изменение"""
        if previous == 0:
            return Decimal('0')
        
        change = ((current - previous) / previous) * 100
        return Decimal(str(round(change, 2)))
    
    @staticmethod
    def calculate_fdv(price: float, max_supply: Optional[int]) -> Optional[Decimal]:
        """Рассчитать полностью разводненную оценку (FDV)"""
        if max_supply is None or price <= 0:
            return None
        
        return Decimal(str(price)) * Decimal(str(max_supply))
    
    @staticmethod
    def calculate_market_cap(price: float, circulating_supply: Optional[int]) -> Optional[Decimal]:
        """Рассчитать рыночную капитализацию"""
        if circulating_supply is None or price <= 0:
            return None
        
        return Decimal(str(price)) * Decimal(str(circulating_supply))
    
    @staticmethod
    def estimate_transactions_count_30d(volume_24h: float, price: float, avg_tx_multiplier: int = 100) -> Optional[int]:
        """Оценить количество транзакций за 30 дней"""
        if volume_24h <= 0 or price <= 0:
            return None
        
        # Предполагаем среднюю транзакцию = цена * множитель
        avg_transaction_value = price * avg_tx_multiplier
        
        if avg_transaction_value <= 0:
            return None
        
        # Транзакции в день = объем / средняя транзакция
        transactions_per_day = volume_24h / avg_transaction_value
        
        # Экстраполируем на 30 дней
        return int(transactions_per_day * 30)
    
    @staticmethod
    def calculate_volume_change_1m(current_volume: float, historical_volumes: Dict[str, float]) -> Optional[int]:
        """Рассчитать изменение объема за месяц"""
        volume_30d_ago = historical_volumes.get('30d')
        
        if volume_30d_ago is None or volume_30d_ago == 0:
            return None
        
        change = ((current_volume - volume_30d_ago) / volume_30d_ago) * 100
        return int(round(change))
    
    @staticmethod
    def calculate_liquidity_score(volume_24h: float, market_cap: float, spread: float = 1.0) -> Decimal:
        """Рассчитать счет ликвидности"""
        if market_cap <= 0:
            return Decimal('0')
        
        # Простая формула: (объем / рыночная кап) * 100 / спред
        volume_to_mcap_ratio = volume_24h / market_cap
        liquidity_score = (volume_to_mcap_ratio * 100) / spread
        
        # Ограничиваем максимальное значение
        return min(Decimal(str(round(liquidity_score, 2))), Decimal('100'))
    
    @staticmethod
    def normalize_social_links(links: Dict[str, Any]) -> Dict[str, str]:
        """Нормализация социальных ссылок"""
        normalized = {
            'website': '',
            'twitter': '',
            'facebook': '',
            'reddit': '',
            'discord': '',
            'instagram': '',
            'medium': '',
            'youtube': '',
            'repositories_link': '',
            'whitepaper_link': ''
        }
        
 
        if isinstance(links, dict):
            # Основной сайт
            if links.get('homepage') and isinstance(links['homepage'], list) and links['homepage']:
                normalized['website'] = links['homepage'][0]
            
            # Twitter
            if links.get('twitter_screen_name'):
                normalized['twitter'] = f"https://twitter.com/{links['twitter_screen_name']}"
            
            # Facebook
            if links.get('facebook_username'):
                normalized['facebook'] = f"https://facebook.com/{links['facebook_username']}"
            
            # Reddit
            if links.get('subreddit_url'):
                normalized['reddit'] = links['subreddit_url']
            
            # GitHub
            repos = links.get('repos_url', {})
            if isinstance(repos, dict) and repos.get('github'):
                if isinstance(repos['github'], list) and repos['github']:
                    normalized['repositories_link'] = repos['github'][0]
            
            # Whitepaper
            if links.get('whitepaper'):
                normalized['whitepaper_link'] = links['whitepaper']
        
        return normalized
    
    @staticmethod
    def safe_decimal_conversion(value: Any, default: Decimal = Decimal('0')) -> Decimal:
 
        if value is None:
            return default
        
        try:
            if isinstance(value, (int, float)):
                return Decimal(str(value))
            elif isinstance(value, str) and value.strip():
                return Decimal(value)
            else:
                return default
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def calculate_coins_count_for_exchange(tickers: list) -> int:
        """Подсчитать количество монет на бирже"""
        if not tickers:
            return 0
        
        unique_coins = set()
        for ticker in tickers:
            if isinstance(ticker, dict):
                base = ticker.get('base')
                target = ticker.get('target')
                if base:
                    unique_coins.add(base.upper())
                if target:
                    unique_coins.add(target.upper())
        
        return len(unique_coins)