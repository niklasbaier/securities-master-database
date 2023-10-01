"""
Holds all constants, api-keys, urls, etc. for the backend.
"""

# Database
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'your_password_here'
DB_DATABASE = 'securities_master'
# DB_DATABASE = 'test'


# Alpaca API
API_URL = 'https://paper-api.alpaca.markets/'
API_KEY = 'your_key_here'
API_SECRET = 'your_secret_here'


# Yahoo Finance
KOI = ['symbol', 'sector', 'industry', 'country', 'website'] # keys of interest
BATCH_SIZE = 500 # max. number of tickers to be included in one batch
