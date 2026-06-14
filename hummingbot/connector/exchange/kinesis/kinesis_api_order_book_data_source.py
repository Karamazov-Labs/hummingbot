import asyncio
import time
import logging
from typing import Any, Dict, List, Optional
from hummingbot.connector.exchange.kinesis import kinesis_constants as CONSTANTS, kinesis_web_utils as web_utils
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

class KinesisAPIOrderBookDataSource(OrderBookTrackerDataSource):
    _logger = None

    @classmethod
    def logger(cls):
        if cls._logger is None:
            cls._logger = logging.getLogger("KinesisAPIOrderBookDataSource")
        return cls._logger

    def __init__(
        self,
        trading_pairs: Optional[List[str]] = None,
        domain: Optional[str] = None,
        api_factory: Optional[WebAssistantsFactory] = None,
    ):
        super().__init__(trading_pairs)
        self._domain = domain or CONSTANTS.DEFAULT_DOMAIN
        self._api_factory = api_factory or web_utils.build_api_factory()

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        prices = {}
        rest_assistant = await self._api_factory.get_rest_assistant()
        for pair in trading_pairs:
            symbol = pair.replace("-", "_")
            url = web_utils.public_rest_url(CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL.format(pair=symbol), domain=self._domain)
            
            resp = await rest_assistant.execute_request(
                url=url,
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            )
            res_json = await resp.json()
            prices[pair] = float(res_json.get("mid_price", 0.0))
        return prices

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        symbol = trading_pair.replace("-", "_")
        url = web_utils.public_rest_url(CONSTANTS.SNAPSHOT_PATH_URL.format(pair=symbol), domain=self._domain)
        rest_assistant = await self._api_factory.get_rest_assistant()
        
        resp = await rest_assistant.execute_request(
            url=url,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
        )
        res_json = await resp.json()
        return res_json.get("depthItems", {})

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot = await self._request_order_book_snapshot(trading_pair)
        
        bids = snapshot.get("bid", [])
        asks = snapshot.get("ask", [])
        
        bids_formatted = [(float(b["price"]), float(b["amount"])) for b in bids]
        asks_formatted = [(float(a["price"]), float(a["amount"])) for a in asks]
        
        timestamp = time.time()
        return OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "update_id": int(timestamp * 1000),
                "bids": bids_formatted,
                "asks": asks_formatted,
            },
            timestamp=timestamp
        )

    async def listen_for_subscriptions(self):
        pass

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        pass

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        while True:
            try:
                for pair in self._trading_pairs:
                    snapshot_msg = await self._order_book_snapshot(pair)
                    output.put_nowait(snapshot_msg)
                await asyncio.sleep(5.0)  # Fetch snapshot every 5 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger().error(f"Error fetching order book snapshot: {e}", exc_info=True)
                await asyncio.sleep(10.0)

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        pass
