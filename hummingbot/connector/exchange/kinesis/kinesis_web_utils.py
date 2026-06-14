import time
from typing import Callable, Optional
import hummingbot.connector.exchange.kinesis.kinesis_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.BASE_URL + path_url

def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.BASE_URL + path_url

def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    return WebAssistantsFactory(
        throttler=throttler,
        auth=auth
    )

def create_throttler() -> AsyncThrottler:
    # Kinesis rate limits: default 10 requests/sec for safe execution
    rate_limits = [
        RateLimit(limit_id=CONSTANTS.EXCHANGE_INFO_PATH_URL, limit=10, time_interval=1.0),
        RateLimit(limit_id=CONSTANTS.SNAPSHOT_PATH_URL, limit=10, time_interval=1.0),
        RateLimit(limit_id=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL, limit=10, time_interval=1.0),
        RateLimit(limit_id=CONSTANTS.ACCOUNTS_BALANCES_PATH_URL, limit=10, time_interval=1.0),
        RateLimit(limit_id=CONSTANTS.ORDERS_PATH_URL, limit=10, time_interval=1.0),
        RateLimit(limit_id=CONSTANTS.ORDER_STATUS_PATH_URL, limit=10, time_interval=1.0),
        RateLimit(limit_id=CONSTANTS.CANCEL_ORDER_PATH_URL, limit=10, time_interval=1.0),
        RateLimit(limit_id=CONSTANTS.MINT_QUOTE_PATH_URL, limit=10, time_interval=1.0),
        RateLimit(limit_id=CONSTANTS.MINT_ORDERS_PATH_URL, limit=10, time_interval=1.0),
    ]
    return AsyncThrottler(rate_limits)
