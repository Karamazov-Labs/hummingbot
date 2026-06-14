# Kinesis Exchange Constants

EXCHANGE_NAME = "kinesis"
DEFAULT_DOMAIN = "com"

# Base REST URL
BASE_URL = "https://api.kinesis.money"

# API Version Suffix
API_VERSION = "v1"

# Public REST Endpoints
EXCHANGE_INFO_PATH_URL = "/v1/exchange/pairs"
SNAPSHOT_PATH_URL = "/v1/exchange/depth/{pair}"
TICKER_PRICE_CHANGE_PATH_URL = "/v1/exchange/mid-price/{pair}"

# Private REST Endpoints
ACCOUNTS_BALANCES_PATH_URL = "/v1/exchange/holdings"
ORDERS_PATH_URL = "/v1/exchange/orders"
ORDER_STATUS_PATH_URL = "/v1/exchange/orders/{id}"
CANCEL_ORDER_PATH_URL = "/v1/exchange/orders/{id}"

# Minting Endpoints (from CCXT additions)
MINT_QUOTE_PATH_URL = "/v1/mint/quote"
MINT_ORDERS_PATH_URL = "/v1/mint/orders"

# WebSocket connection URL
WS_URL = "wss://fastapi.kinesis.money"
