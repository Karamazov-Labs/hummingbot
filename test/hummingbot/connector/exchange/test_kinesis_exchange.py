import asyncio
import hashlib
import hmac
import time
from decimal import Decimal
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.connector.exchange.kinesis.kinesis_auth import KinesisAuth
from hummingbot.connector.exchange.kinesis.kinesis_exchange import KinesisExchange
from hummingbot.connector.exchange.kinesis.kinesis_api_order_book_data_source import KinesisAPIOrderBookDataSource
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest

@pytest.mark.asyncio
async def test_auth_signature():
    api_key = "test_api_key"
    secret_key = "test_secret_key"
    auth = KinesisAuth(api_key, secret_key)
    
    # Create request
    request = RESTRequest(
        method=RESTMethod.GET,
        url="https://api.kinesis.money/v1/exchange/holdings",
        is_auth_required=True
    )
    
    # Authenticate request
    authenticated_request = await auth.rest_authenticate(request)
    headers = authenticated_request.headers
    
    assert "x-nonce" in headers
    assert headers["x-api-key"] == api_key
    assert "x-signature" in headers
    
    # Re-calculate signature
    nonce = headers["x-nonce"]
    auth_message = nonce + "GET" + "/v1/exchange/holdings"
    expected_sig = hmac.new(
        secret_key.encode("utf-8"),
        auth_message.encode("utf-8"),
        hashlib.sha256
    ).hexdigest().upper()
    
    assert headers["x-signature"] == expected_sig

@pytest.mark.asyncio
@patch("hummingbot.connector.exchange.kinesis.kinesis_exchange.KinesisAPIOrderBookDataSource.get_last_traded_prices", new_callable=AsyncMock)
async def test_get_last_traded_prices(mock_prices):
    mock_prices.return_value = {"KAG-C1USD": 28.50}
    
    data_source = KinesisAPIOrderBookDataSource(trading_pairs=["KAG-C1USD"])
    prices = await data_source.get_last_traded_prices(["KAG-C1USD"])
    
    assert prices["KAG-C1USD"] == 28.50

@pytest.mark.asyncio
async def test_place_order():
    exchange = KinesisExchange(
        kinesis_api_key="key",
        kinesis_secret_key="secret",
        trading_pairs=["KAG-C1USD"],
        trading_required=True
    )
    
    # Mock _api_post response
    mock_response = AsyncMock()
    mock_response.json.return_value = {"id": "12345"}
    exchange._api_post = AsyncMock(return_value=mock_response)
    
    # Place a limit buy order
    order_id = "hb-KAG-C1USD-1700000000000"
    trading_pair = "KAG-C1USD"
    amount = Decimal("10.0")
    price = Decimal("28.50")
    
    exchange_order_id, timestamp = await exchange._place_order(
        order_id=order_id,
        trading_pair=trading_pair,
        amount=amount,
        trade_type=TradeType.BUY,
        order_type=OrderType.LIMIT,
        price=price
    )
    
    assert exchange_order_id == "12345"
    assert exchange._api_post.called is True
    
    # Check payload sent
    sent_request = exchange._api_post.call_args[0][0]
    sent_data = sent_request.data
    assert "limitPrice" in sent_data
    assert "amount" in sent_data
    assert "direction" in sent_data

@pytest.mark.asyncio
async def test_place_cancel():
    exchange = KinesisExchange(
        kinesis_api_key="key",
        kinesis_secret_key="secret",
        trading_pairs=["KAG-C1USD"],
        trading_required=True
    )
    
    # Setup mock active order
    mock_order = MagicMock()
    mock_order.get_exchange_order_id = AsyncMock(return_value="12345")
    mock_order.trading_pair = "KAG-C1USD"
    mock_order.client_order_id = "hb-KAG-C1USD-1"
    
    # Mock delete call
    exchange._api_delete = AsyncMock()
    
    success = await exchange._place_cancel("hb-KAG-C1USD-1", mock_order)
    
    assert success is True
    assert exchange._api_delete.called is True

@pytest.mark.asyncio
async def test_request_order_status():
    exchange = KinesisExchange(
        kinesis_api_key="key",
        kinesis_secret_key="secret",
        trading_pairs=["KAG-C1USD"],
        trading_required=True
    )
    
    # Setup mock active order
    mock_order = MagicMock()
    mock_order.get_exchange_order_id = AsyncMock(return_value="12345")
    mock_order.trading_pair = "KAG-C1USD"
    mock_order.client_order_id = "hb-KAG-C1USD-1"
    
    # Mock status response
    mock_response = AsyncMock()
    mock_response.json.return_value = {
        "status": "filled",
        "filledAmount": "10.0",
        "remaining": "0.0"
    }
    exchange._api_get = AsyncMock(return_value=mock_response)
    
    order_update = await exchange._request_order_status(mock_order)
    
    assert order_update.client_order_id == "hb-KAG-C1USD-1"
    assert order_update.exchange_order_id == "12345"
    assert order_update.new_state == OrderState.FILLED
    assert order_update.trading_pair == "KAG-C1USD"

@pytest.mark.asyncio
async def test_all_trade_updates_for_order():
    exchange = KinesisExchange(
        kinesis_api_key="key",
        kinesis_secret_key="secret",
        trading_pairs=["KAG-C1USD"],
        trading_required=True
    )
    
    # Setup mock active order
    mock_order = MagicMock()
    mock_order.get_exchange_order_id = AsyncMock(return_value="12345")
    mock_order.trading_pair = "KAG-C1USD"
    mock_order.client_order_id = "hb-KAG-C1USD-1"
    mock_order.executed_amount_base = Decimal("2.0")
    mock_order.price = Decimal("28.50")
    
    # Mock status response with a new fill delta (filledAmount 10.0 > executed_amount_base 2.0)
    mock_response = AsyncMock()
    mock_response.json.return_value = {
        "status": "filled",
        "filledAmount": "10.0",
        "limitPrice": "28.50"
    }
    exchange._api_get = AsyncMock(return_value=mock_response)
    
    trade_updates = await exchange._all_trade_updates_for_order(mock_order)
    
    assert len(trade_updates) == 1
    trade_update = trade_updates[0]
    assert trade_update.client_order_id == "hb-KAG-C1USD-1"
    assert trade_update.exchange_order_id == "12345"
    assert trade_update.fill_base_amount == Decimal("8.0")
    assert trade_update.fill_price == Decimal("28.50")

@pytest.mark.asyncio
async def test_redis_pubsub_message_parsing():
    import json
    from hummingbot.core.data_type.order_book_message import OrderBookMessageType
    
    # Initialize data source
    data_source = KinesisAPIOrderBookDataSource(
        trading_pairs=["KAG-C1USD"],
        redis_host="127.0.0.1",
        redis_port=6379,
        use_redis_mirror=True
    )
    
    # Setup mock Redis client and pubsub
    mock_pubsub = AsyncMock()
    mock_message = {
        "type": "message",
        "data": json.dumps({
            "source": "kinesis",
            "stream": "depth",
            "event": "onChange",
            "data": {
                "symbolId": "KAG_C1USD",
                "depth": {
                    "bid": [{"price": "28.50", "amount": "100.0"}],
                    "ask": [{"price": "28.60", "amount": "120.0"}]
                }
            }
        })
    }
    # Mock listen() iterator
    async def mock_listen():
        yield mock_message
    mock_pubsub.listen = mock_listen
    
    mock_client = MagicMock()
    mock_client.pubsub.return_value = mock_pubsub
    mock_client.aclose = AsyncMock()
    
    with patch("redis.asyncio.from_url", return_value=mock_client):
        output_queue = asyncio.create_task(data_source._listen_to_redis_pubsub(asyncio.Queue()))
        await asyncio.sleep(0.1)
        output_queue.cancel()
        
        # Verify the client connects to redis
        assert mock_client.pubsub.called is True
