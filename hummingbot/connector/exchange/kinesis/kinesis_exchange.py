import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from bidict import bidict

from hummingbot.connector.exchange.kinesis import kinesis_constants as CONSTANTS, kinesis_utils as utils, kinesis_web_utils as web_utils
from hummingbot.connector.exchange.kinesis.kinesis_auth import KinesisAuth
from hummingbot.connector.exchange.kinesis.kinesis_api_order_book_data_source import KinesisAPIOrderBookDataSource
from hummingbot.connector.exchange.kinesis.kinesis_api_user_stream_data_source import KinesisAPIUserStreamDataSource
from hummingbot.connector.exchange.kinesis.kinesis_order_book_tracker import KinesisOrderBookTracker
from hummingbot.connector.exchange.kinesis.kinesis_user_stream_tracker import KinesisUserStreamTracker
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OpenOrder, OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

s_decimal_NaN = Decimal("nan")
s_decimal_0 = Decimal(0)

class KinesisExchange(ExchangePyBase):
    """
    Hummingbot Spot Exchange Connector for Kinesis.
    """
    web_utils = web_utils

    def __init__(
        self,
        kinesis_api_key: str,
        kinesis_secret_key: str,
        kinesis_redis_host: str = "127.0.0.1",
        kinesis_redis_port: int = 6379,
        kinesis_redis_password: Optional[str] = None,
        use_redis_mirror: bool = False,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: Optional[str] = None,
    ):
        self._api_key = kinesis_api_key
        self._secret_key = kinesis_secret_key
        self._redis_host = kinesis_redis_host
        self._redis_port = kinesis_redis_port
        self._redis_password = kinesis_redis_password
        self._use_redis_mirror = use_redis_mirror
        self._domain = domain or CONSTANTS.DEFAULT_DOMAIN
        self._trading_pairs = trading_pairs
        self._trading_required = trading_required
        
        self._authenticator = KinesisAuth(self._api_key, self._secret_key)
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def authenticator(self):
        return self._authenticator

    @property
    def domain(self):
        return self._domain

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def rate_limits_rules(self):
        return web_utils.create_throttler().rate_limits

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def check_network_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def client_order_id_max_length(self):
        return 64

    @property
    def client_order_id_prefix(self):
        return utils.HUMMINGBOT_ID_PREFIX + "-"

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.MARKET, OrderType.LIMIT]

    async def _update_trading_fees(self):
        pass

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._authenticator
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return KinesisAPIOrderBookDataSource(
            trading_pairs=self.trading_pairs,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
            redis_host=self._redis_host,
            redis_port=self._redis_port,
            redis_password=self._redis_password,
            use_redis_mirror=self._use_redis_mirror
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return KinesisAPIUserStreamDataSource(
            domain=self.domain,
            api_factory=self._web_assistants_factory
        )

    def _create_order_book_tracker(self) -> KinesisOrderBookTracker:
        return KinesisOrderBookTracker(
            trading_pairs=self.trading_pairs,
            domain=self.domain,
            api_factory=self._web_assistants_factory,
            redis_host=self._redis_host,
            redis_port=self._redis_port,
            redis_password=self._redis_password,
            use_redis_mirror=self._use_redis_mirror
        )

    def _create_user_stream_tracker(self) -> KinesisUserStreamTracker:
        return KinesisUserStreamTracker(
            domain=self.domain,
            api_factory=self._web_assistants_factory,
            auth=self._authenticator
        )

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return "not found" in str(status_update_exception).lower() or "not_found" in str(status_update_exception).lower()

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return "not found" in str(cancelation_exception).lower() or "not_found" in str(cancelation_exception).lower()

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]):
        mapping = bidict()
        for pair_info in exchange_info:
            exchange_symbol = pair_info["currencyPairId"]
            base = pair_info["baseCurrency"]
            quote = pair_info["quoteCurrency"]
            trading_pair = f"{base}-{quote}"
            mapping[exchange_symbol] = trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs
    ) -> Tuple[str, float]:
        """
        Submits order creation REST API call to Kinesis Exchange.
        """
        symbol = utils.trading_pair_to_exchange_symbol(trading_pair)
        
        payload = {
            "currencyPairId": symbol,
            "direction": "buy" if trade_type == TradeType.BUY else "sell",
            "amount": float(amount),
            "orderType": "market" if order_type == OrderType.MARKET else "limit"
        }
        
        if order_type == OrderType.LIMIT:
            payload["limitPrice"] = float(price)

        request = RESTRequest(
            method=RESTMethod.POST,
            url=web_utils.private_rest_url(CONSTANTS.ORDERS_PATH_URL, domain=self.domain),
            data=self.web_utils.json.dumps(payload),
            is_auth_required=True
        )
        
        response = await self._api_post(request)
        res_json = await response.json()
        
        exchange_order_id = str(res_json.get("id"))
        return exchange_order_id, self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        """
        Submits order cancellation REST API call to Kinesis Exchange.
        """
        exchange_order_id = await tracked_order.get_exchange_order_id()
        cancel_url = CONSTANTS.CANCEL_ORDER_PATH_URL.format(id=exchange_order_id)

        request = RESTRequest(
            method=RESTMethod.DELETE,
            url=web_utils.private_rest_url(cancel_url, domain=self.domain),
            is_auth_required=True
        )
        
        await self._api_delete(request)
        return True

    def _map_order_state(self, status: str) -> OrderState:
        normalized = (status or "").lower()
        if normalized in {"filled", "complete", "completed"}:
            return OrderState.FILLED
        if normalized in {"cancelled", "canceled", "cancelled_by_user", "cancelled_by_system"}:
            return OrderState.CANCELED
        if normalized in {"rejected", "failed", "expired"}:
            return OrderState.FAILED
        return OrderState.OPEN

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Queries Kinesis REST API order endpoint to get status.
        """
        exchange_order_id = await tracked_order.get_exchange_order_id()
        status_url = CONSTANTS.ORDER_STATUS_PATH_URL.format(id=exchange_order_id)

        request = RESTRequest(
            method=RESTMethod.GET,
            url=web_utils.private_rest_url(status_url, domain=self.domain),
            is_auth_required=True
        )
        
        response = await self._api_get(request)
        res_json = await response.json()
        
        status = res_json.get("status", "open")
        
        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
            new_state=self._map_order_state(status),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp
        )
        return order_update

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        Queries Kinesis REST API order endpoint to get status and returns trade updates.
        """
        trade_updates = []
        try:
            exchange_order_id = await order.get_exchange_order_id()
            status_url = CONSTANTS.ORDER_STATUS_PATH_URL.format(id=exchange_order_id)

            request = RESTRequest(
                method=RESTMethod.GET,
                url=web_utils.private_rest_url(status_url, domain=self.domain),
                is_auth_required=True
            )
            
            response = await self._api_get(request)
            res_json = await response.json()
            
            filled_amount = Decimal(str(res_json.get("filledAmount", "0")))
            
            if filled_amount > order.executed_amount_base:
                new_fill_amount = filled_amount - order.executed_amount_base
                price = Decimal(str(res_json.get("limitPrice", order.price or "0")))
                if price == s_decimal_0 and order.price:
                    price = order.price
                
                fee = DeductedFromReturnsTradeFee(percent=Decimal("0.0022"))
                
                trade_update = TradeUpdate(
                    trade_id=f"{exchange_order_id}-{int(self.current_timestamp * 1000)}",
                    client_order_id=order.client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=order.trading_pair,
                    fill_timestamp=self.current_timestamp,
                    fill_price=price,
                    fill_base_amount=new_fill_amount,
                    fill_quote_amount=new_fill_amount * price,
                    fee=fee,
                    is_taker=True
                )
                trade_updates.append(trade_update)
        except Exception as e:
            self.logger().warning(f"Failed to fetch trade updates for order {order.client_order_id}: {e}")
        return trade_updates

    async def _update_balances(self):
        """
        Pulls holdings from Kinesis Exchange and updates memory balance caches.
        """
        request = RESTRequest(
            method=RESTMethod.GET,
            url=web_utils.private_rest_url(CONSTANTS.ACCOUNTS_BALANCES_PATH_URL, domain=self.domain),
            is_auth_required=True
        )
        
        response = await self._api_get(request)
        res_json = await response.json()
        
        # Clear local balances and repopulate
        self._account_balances.clear()
        self._account_available_balances.clear()
        
        for currency_id, entry in res_json.items():
            asset_code = utils.exchange_symbol_to_trading_pair(currency_id)  # Returns code
            available = Decimal(str(entry.get("available", "0")))
            allocated = Decimal(str(entry.get("allocatedOnExchange", "0")))
            total = available + allocated
            
            self._account_balances[asset_code] = total
            self._account_available_balances[asset_code] = available

    async def _format_trading_rules(self, raw_trading_pair_info: List[Dict[str, Any]]) -> List[TradingRule]:
        """
        Formats trading pair details to Hummingbot TradingRules.
        """
        rules = []
        for pair_info in raw_trading_pair_info:
            trading_pair = utils.exchange_symbol_to_trading_pair(pair_info["currencyPairId"])
            
            price_precision = int(pair_info.get("pricePrecision", 4))
            amount_precision = int(pair_info.get("amountPrecision", 2))
            
            tick_size = Decimal("1") / (Decimal("10") ** price_precision)
            step_size = Decimal("1") / (Decimal("10") ** amount_precision)
            min_amount = Decimal(str(pair_info.get("minAmount", "0.01")))
            
            rules.append(
                TradingRule(
                    trading_pair=trading_pair,
                    min_order_size=min_amount,
                    min_price_increment=tick_size,
                    min_base_amount_increment=step_size,
                    supports_limit_orders=True,
                    supports_market_orders=True
                )
            )
        return rules

    def _get_fee(
        self,
        base_asset: str,
        quote_asset: str,
        order_type: OrderType,
        order_side: TradeType,
        amount: Decimal,
        price: Decimal,
        is_maker: Optional[bool] = None
    ) -> TradeFeeBase:
        # Kinesis uses symmetric 0.22% fee
        return DeductedFromReturnsTradeFee(percent=Decimal("0.0022"))

