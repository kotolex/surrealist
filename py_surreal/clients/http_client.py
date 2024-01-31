import base64
import json
import urllib.parse
import urllib.request
from http.client import HTTPResponse, RemoteDisconnected
from logging import getLogger
from typing import Optional, Tuple, Dict, Union
from urllib.error import URLError, HTTPError

from py_surreal.errors import HttpClientError
from py_surreal.utils import ENCODING, DEFAULT_TIMEOUT, crop_data, mask_pass

logger = getLogger("http_client")


class HttpClient:
    """
    Http-client for working with http endpoints and abilities of SurrealDB
    """

    def __init__(self, base_url: str, headers: Optional[Dict] = None, credentials: Optional[Tuple[str, str]] = None,
                 timeout: int = DEFAULT_TIMEOUT):
        self._base_url = base_url
        self.credentials = credentials
        self.timeout = timeout
        headers = headers or {}
        self._headers = {"Accept": "application/json", "User-Agent": "py_surreal http-client", **headers}
        if credentials:
            self._user, self._pass = credentials
            base64string = base64.encodebytes(f'{self._user}:{self._pass}'.encode(ENCODING))[:-1]
            self._headers["Authorization"] = f"Basic {base64string.decode(ENCODING)}"

    def get(self, path: str = '') -> HTTPResponse:
        return self.request("GET", None, path)

    def post(self, data: Dict, path: str = '') -> HTTPResponse:
        return self.request("POST", data, path)

    def put(self, data: Dict, path: str = '') -> HTTPResponse:
        return self.request("PUT", data, path)

    def patch(self, data: Dict, path: str = '') -> HTTPResponse:
        return self.request("PATCH", data, path)

    def request(self, method: str, data: Optional[Union[Dict, str]], path: str = '',
                not_json: bool = False) -> HTTPResponse:
        response = None
        url = f'{self._base_url}{path}'
        options = {'method': method, 'headers': self._headers}
        if method not in ("GET", "DELETE"):
            js = json.dumps(data).encode(ENCODING) if not not_json else data.encode(ENCODING)
            options['data'] = js
        try:
            req = urllib.request.Request(url, **options)
            logger.debug("Request to %s, options: %s, timeout: %d", url, mask_opts(options), self.timeout)
            response = urllib.request.urlopen(req, timeout=self.timeout)
            return response
        except HTTPError as e:
            logger.error("Http error on request %s, info: %s", url, e)
            if response:
                response.close()
            return e
        except (URLError, RemoteDisconnected, ConnectionResetError) as e:
            if response:
                response.close()
            logger.error("Error on connecting to %s, info: %s", url, e)
            raise HttpClientError(f"Error on connecting to '{url}'")



def mask_opts(options: Dict) -> Dict:
    masked_opts = {}
    for key, value in options.items():
        if key == "headers" and "Authorization" in value:
            value = {**value, "Authorization": "Basic ******"}
        elif key == "data":
            value = crop_data(mask_pass(str(value)))
        masked_opts[key] = value
    return masked_opts
