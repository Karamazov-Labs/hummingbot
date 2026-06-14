from typing import List, Optional
from hummingbot.connector.exchange.kinesis import kinesis_constants as CONSTANTS
from hummingbot.connector.exchange.kinesis.kinesis_api_order_book_data_source import KinesisAPIOrderBookDataSource
from hummingbot.core.data_type.order_book_tracker import OrderBookTracker
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

class KinesisOrderBookTracker(OrderBookTracker):
    def __init__(
        self,
        trading_pairs: Optional[List[str]] = None,
        domain: Optional[str] = None,
        api_factory: Optional[WebAssistantsFactory] = None,
        redis_host: str = "127.0.0.1",
        redis_port: int = 6379,
        redis_password: Optional[str] = None,
        use_redis_mirror: bool = False,
    ):
        super().__init__(
            data_source=KinesisAPIOrderBookDataSource(
                trading_pairs=trading_pairs,
                domain=domain,
                api_factory=api_factory,
                redis_host=redis_host,
                redis_port=redis_port,
                redis_password=redis_password,
                use_redis_mirror=use_redis_mirror
            ),
            trading_pairs=trading_pairs
        )
        self._domain = domain or CONSTANTS.DEFAULT_DOMAIN
