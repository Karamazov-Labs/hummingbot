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
async def test_execute_cancel():
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
    exchange._order_tracker.active_orders["hb-KAG-C1USD-1"] = mock_order
    
    # Mock delete call
    exchange._api_delete = AsyncMock()
    
    cancel_order_id = await exchange._execute_cancel("hb-KAG-C1USD-1", "KAG-C1USD")
    
    assert cancel_order_id == "hb-KAG-C1USD-1"
    assert exchange._api_delete.called is True
