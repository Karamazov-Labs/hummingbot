import asyncio
import time
import logging
import json
import redis.asyncio as aioredis
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
        redis_host: str = "127.0.0.1",
        redis_port: int = 6379,
        redis_password: Optional[str] = None,
        use_redis_mirror: bool = False,
    ):
        super().__init__(trading_pairs)
        self._domain = domain or CONSTANTS.DEFAULT_DOMAIN
        self._api_factory = api_factory or web_utils.build_api_factory()
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_password = redis_password
        self._use_redis_mirror = use_redis_mirror
        self._redis_task = None

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

    async def _listen_to_redis_pubsub(self, output: asyncio.Queue):
        """
        Connects to Redis and streams depth snapshots from the kinesis_depth_stream channel in real time.
        """
        password_part = f":{self._redis_password}@" if self._redis_password else ""
        redis_url = f"redis://{password_part}{self._redis_host}:{self._redis_port}"
        
        client = aioredis.from_url(redis_url, decode_responses=True)
        try:
            pubsub = client.pubsub()
            await pubsub.subscribe("kinesis_depth_stream")
            
            async for message in pubsub.listen():
                if not self._use_redis_mirror:
                    break
                if message["type"] == "message":
                    data = json.loads(message["data"])
                    event_data = data.get("data", {})
                    symbol_id = event_data.get("symbolId")
                    if symbol_id:
                        trading_pair = symbol_id.replace("_", "-").replace("/", "-")
                        if trading_pair in self._trading_pairs:
                            depth_data = event_data.get("depth", {})
                            bids = depth_data.get("bid", [])
                            asks = depth_data.get("ask", [])
                            
                            bids_formatted = [(float(b["price"]), float(b["amount"])) for b in bids]
                            asks_formatted = [(float(a["price"]), float(a["amount"])) for a in asks]
                            
                            timestamp = time.time()
                            snapshot_msg = OrderBookMessage(
                                message_type=OrderBookMessageType.SNAPSHOT,
                                content={
                                    "trading_pair": trading_pair,
                                    "update_id": int(timestamp * 1000),
                                    "bids": bids_formatted,
                                    "asks": asks_formatted,
                                },
                                timestamp=timestamp
                            )
                            output.put_nowait(snapshot_msg)
        finally:
            await client.aclose()

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        if self._use_redis_mirror:
            try:
                self._redis_task = asyncio.create_task(self._listen_to_redis_pubsub(output))
                # Small sleep to check if the connection failed instantly
                await asyncio.sleep(0.5)
                if self._redis_task.done() and self._redis_task.exception():
                    raise self._redis_task.exception()
                self.logger().info("Successfully connected to kinesis_stream Redis mirror for live pricing.")
                await self._redis_task
                return
            except Exception as e:
                self.logger().error(f"Failed to connect to Redis mirror ({e}). Falling back to REST polling.")
                if self._redis_task and not self._redis_task.done():
                    self._redis_task.cancel()
        
        # Fallback REST Polling Loop
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
