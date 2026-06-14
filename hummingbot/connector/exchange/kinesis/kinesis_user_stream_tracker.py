from typing import Optional
from hummingbot.connector.exchange.kinesis.kinesis_api_user_stream_data_source import KinesisAPIUserStreamDataSource
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.auth import AuthBase

class KinesisUserStreamTracker(UserStreamTracker):
    def __init__(
        self,
        domain: Optional[str] = None,
        api_factory: Optional[WebAssistantsFactory] = None,
        auth: Optional[AuthBase] = None,
    ):
        super().__init__(
            data_source=KinesisAPIUserStreamDataSource(
                domain=domain,
                api_factory=api_factory
            )
        )
        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
