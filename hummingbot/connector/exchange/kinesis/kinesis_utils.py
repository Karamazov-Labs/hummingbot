from typing import Any, Dict
from pydantic import ConfigDict, Field, SecretStr
from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.utils.tracking_nonce import get_tracking_nonce

CENTRALIZED = True
EXAMPLE_PAIR = "KAG-C1USD"
HUMMINGBOT_ID_PREFIX = "hb"

# Kinesis fees are 0.22% maker/taker
DEFAULT_FEES = [0.22, 0.22]

def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Verifies if a trading pair is enabled to operate based on its exchange info.
    """
    # Kinesis pairs are active if they are returned by exchange/pairs
    return exchange_info.get("active", True)

def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    ts_micro_sec: int = get_tracking_nonce()
    return f"{HUMMINGBOT_ID_PREFIX}-{trading_pair}-{ts_micro_sec}"

def trading_pair_to_exchange_symbol(trading_pair: str) -> str:
    """
    Translates Hummingbot trading pair e.g. KAG-C1USD to Kinesis style KAG_C1USD
    """
    return trading_pair.replace("-", "_")

def exchange_symbol_to_trading_pair(symbol: str) -> str:
    """
    Translates Kinesis style symbol e.g. KAG_C1USD or KAG/C1USD to KAG-C1USD
    """
    for separator in ["_", "/"]:
        if separator in symbol:
            return symbol.replace(separator, "-")
    return symbol

from typing import Optional

class KinesisConfigMap(BaseConnectorConfigMap):
    connector: str = "kinesis"
    kinesis_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Kinesis API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    kinesis_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Kinesis secret key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    kinesis_redis_host: str = Field(
        default="127.0.0.1",
        json_schema_extra={
            "prompt": "Enter Redis Host for kinesis_stream mirror",
            "is_connect_key": True,
        }
    )
    kinesis_redis_port: int = Field(
        default=6379,
        json_schema_extra={
            "prompt": "Enter Redis Port for kinesis_stream mirror",
            "is_connect_key": True,
        }
    )
    kinesis_redis_password: Optional[SecretStr] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter Redis Password (optional)",
            "is_secure": True,
            "is_connect_key": True,
        }
    )
    use_redis_mirror: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": "Use local Redis mirror for WebSocket live pricing? (True/False)",
            "is_connect_key": True,
        }
    )
    model_config = ConfigDict(title="kinesis")

KEYS = KinesisConfigMap.model_construct()
