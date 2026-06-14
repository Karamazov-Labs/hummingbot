import hashlib
import hmac
import time
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest

class KinesisAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str):
        """
        API key HMAC payload signing for Kinesis KMS REST requests.
        """
        self._api_key = api_key
        self._secret_key = secret_key

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the x-nonce, x-api-key, and x-signature headers required for authenticated interactions.
        """
        nonce = str(int(time.time() * 1000))
        method = request.method.name
        
        # Build path and query
        url_path = request.url
        if "://" in url_path:
            path_part = url_path.split("://", 1)[1]
            if "/" in path_part:
                url_path = "/" + path_part.split("/", 1)[1]
            else:
                url_path = "/"
        
        auth_message = nonce + method + url_path
        
        if request.data:
            auth_message += request.data

        # Generate HMAC-SHA256 signature in uppercase
        signature = hmac.new(
            self._secret_key.encode("utf-8"),
            auth_message.encode("utf-8"),
            hashlib.sha256
        ).hexdigest().upper()

        headers = {
            "x-nonce": nonce,
            "x-api-key": self._api_key,
            "x-signature": signature,
        }
        
        if method != "DELETE":
            headers["Content-Type"] = "application/json"
            
        if request.headers:
            request.headers.update(headers)
        else:
            request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request
