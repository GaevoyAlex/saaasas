import os
from typing import List
from dotenv import load_dotenv

load_dotenv()

class Config:
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    DYNAMODB_TABLE = os.getenv('DYNAMODB_TABLE', 'crypto_data')
    
    COINGECKO_API_URL = 'https://api.coingecko.com/api/v3'
    COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
    
    RATE_LIMIT_REQUESTS = int(os.getenv('RATE_LIMIT_REQUESTS', 10))
    RATE_LIMIT_WINDOW = int(os.getenv('RATE_LIMIT_WINDOW', 60))
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 10))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    
    TOP_EXCHANGES_LIMIT = int(os.getenv('TOP_EXCHANGES_LIMIT', 50))
    TOP_TOKENS_LIMIT = int(os.getenv('TOP_TOKENS_LIMIT', 1000))
    
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = 'logs/parser.log'
    
    TTL_DAYS_STATS = int(os.getenv('TTL_DAYS_STATS', 7))
    TTL_DAYS_INFO = int(os.getenv('TTL_DAYS_INFO', 30))