import base64
import json
import urllib.parse
import urllib.request
from http.client import HTTPResponse
from typing import Optional, Tuple, Dict, Union
from urllib.error import URLError, HTTPError

from .const import ENCODING


class HttpClient:
    def __init__(self, base_url: str, headers: Optional[Dict] = None, credentials: Optional[Tuple[str, str]] = None,
                 timeout: int = 5):
        self._base_url = base_url
        self.credentials = credentials
        self.timeout = timeout
        headers = headers or {}
        self._headers = {"Accept": "application/json", **headers}
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
        try:
            if method == "GET":
                req = urllib.request.Request(url, headers=self._headers)
            elif method == "DELETE":
                req = urllib.request.Request(url, method=method, headers=self._headers)
            else:
                js = json.dumps(data).encode(ENCODING) if not not_json else data.encode(ENCODING)
                req = urllib.request.Request(url, method=method, headers=self._headers, data=js)
            response = urllib.request.urlopen(req, timeout=self.timeout)
            return response
        except HTTPError as e:
            return e.fp
        except URLError:
            if response:
                response.close()
            raise ValueError(f"Error on connecting to '{url}'")
