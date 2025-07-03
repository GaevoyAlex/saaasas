class CoinGeckoParserException(Exception):
    pass

class APIException(CoinGeckoParserException):
    pass

class DataValidationException(CoinGeckoParserException):
    pass

class DatabaseException(CoinGeckoParserException):
    pass

class ConfigurationException(CoinGeckoParserException):
    pass

class RateLimitException(CoinGeckoParserException):
    pass