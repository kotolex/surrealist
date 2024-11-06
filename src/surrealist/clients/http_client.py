import json
import urllib.parse
import urllib.request
from http.client import HTTPResponse, RemoteDisconnected
from logging import getLogger
from typing import Optional, Tuple, Dict, Union, BinaryIO
from urllib.error import URLError, HTTPError

from surrealist.errors import HttpClientError, TooManyNestedLevelsError
from surrealist.utils import ENCODING, DEFAULT_TIMEOUT, mask_pass, NS, DB, AC

logger = getLogger("surrealist.clients.http")


class HttpClient:
    """
    Http-client for working with http endpoints and abilities of SurrealDB
    """

    def __init__(self, base_url: str, headers: Optional[Dict] = None, credentials: Optional[Tuple[str, str]] = None,
                 timeout: int = DEFAULT_TIMEOUT):
        self._base_url = base_url
        self._credentials = credentials
        self._timeout = timeout
        headers = headers or {}
        headers = {k: v for k, v in headers.items() if v is not None}
        headers = {k if k not in (NS, DB) else f"surreal-{k}": v for k, v in headers.items()}
        self._headers = {"Content-Type": "application/json", "Accept": "application/json",
                         "User-Agent": "surrealist http-client", **headers}
        self._token = None

    def set_token(self, token: str) -> None:
        """
        Stores token for future requests
        :param token: token to use
        """
        self._token = token
        self._headers["Authorization"] = f"Bearer {token}"

    def set_db_params(self, params: Dict) -> None:
        """
        Stores params for future requests
        :param params: params to use (ns, db, ac)
        """
        # We have to remove `db`, 'ns' and `ac` params from headers, before add new
        self._headers = {k: v for k, v in self._headers.items() if k not in (DB, AC, NS)}
        self._headers = {**self._headers, **params}
        self._headers = {k if k not in (NS, DB) else f"surreal-{k}": v for k, v in self._headers.items()}

    def get(self, path: str = '') -> HTTPResponse:
        """
        Represents GET method
        :param path: endpoint to request
        :return: response
        """
        return self.request("GET", None, path)

    def post(self, data: Dict, path: str = '') -> HTTPResponse:
        """
        Represents POST method
        :param data: json or bytes data
        :param path: endpoint to request
        :return: response
        """
        return self.request("POST", data, path)

    def put(self, data: Dict, path: str = '') -> HTTPResponse:
        """
        Represents POST method
        :param data: json or bytes data
        :param path: endpoint to request
        :return: response
        """
        return self.request("PUT", data, path)

    def patch(self, data: Dict, path: str = '') -> HTTPResponse:
        """
        Represents POST method
        :param data: json or bytes data
        :param path: endpoint to request
        :return: response
        """
        return self.request("PATCH", data, path)

    def request(self, method: str, data: Optional[Union[Dict, str, BinaryIO]], path: str = '',
                type_of_content: str = "JSON") -> Union[HTTPResponse, HTTPError]:
        """
        Main method to perform all kinds of requests

        :param method: method name
        :param data: data to send
        :param path: endpoint
        :param type_of_content: flag to handle data
        :return: response to use
        """
        response = None
        url = f'{self._base_url}{path}'
        options = {'method': method, 'headers': self._headers}
        if method not in ("GET", "DELETE"):
            if type_of_content == "JSON":
                try:
                    data_to_send = json.dumps(data, ensure_ascii=False).encode(ENCODING)
                except RecursionError as e:
                    logger.error("Cant serialize object, too many nested levels")
                    raise TooManyNestedLevelsError("Cant serialize object, too many nested levels") from e
            elif type_of_content == "STR":
                data_to_send = data.encode(ENCODING)
            else:  # it is a file-like object (BinaryIO)
                data_to_send = data
            options['data'] = data_to_send
        try:
            req = urllib.request.Request(url, **options)
            logger.debug("Request to %s, options: %s, timeout: %d", url, mask_opts(options), self._timeout)
            response = urllib.request.urlopen(req, timeout=self._timeout)
            return response
        except HTTPError as e:
            logger.error("Http error on request %s, info: %s", url, e)
            if response:
                response.close()
            # we need it to check status code and text on error
            return e
        except (URLError, RemoteDisconnected, ConnectionResetError) as e:
            if response:
                response.close()
            logger.error("Error on connecting to %s, info: %s", url, e)
            raise HttpClientError(f"Error on connecting to '{url}'") from e


def mask_opts(options: Dict) -> Dict:
    """
    Hide authorization data with asterisks, so "Authorization": "Basic ***" will be on return

    :param options: dict with options(headers)
    :return: same dict or dict with hid auth data
    """
    masked_opts = {}
    for key, value in options.items():
        if key == "headers" and "Authorization" in value:
            value = {**value, "Authorization": "Bearer ******"}
        elif key == "data":
            value = mask_pass(str(value))
        masked_opts[key] = value
    return masked_opts
