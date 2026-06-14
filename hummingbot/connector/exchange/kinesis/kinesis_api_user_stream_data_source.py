import asyncio
from typing import Optional
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

class KinesisAPIUserStreamDataSource(UserStreamTrackerDataSource):
    def __init__(
        self,
        domain: Optional[str] = None,
        api_factory: Optional[WebAssistantsFactory] = None,
    ):
        super().__init__()
        self._domain = domain
        self._api_factory = api_factory

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Kinesis API does not provide a public user event websocket.
        We run an infinite sleep loop to maintain the Hummingbot tracker interface.
        """
        while True:
            await asyncio.sleep(3600)
