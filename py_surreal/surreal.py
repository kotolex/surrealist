from typing import Tuple, Optional

from py_surreal.connection import Connection
from py_surreal.http_connection import HttpConnection
from py_surreal.utils import DEFAULT_TIMEOUT
from py_surreal.ws_connection import WebSocketConnection


class Surreal:
    def __init__(self, url: str, namespace: Optional[str] = None, database: Optional[str] = None,
                 credentials: Tuple[str, str] = None, use_http: bool = False, timeout: int = DEFAULT_TIMEOUT):
        self.client = HttpConnection if use_http else WebSocketConnection
        self.db_params = {}
        if namespace:
            self.db_params["NS"] = namespace
        if database:
            self.db_params["DB"] = database
        if not self.db_params:
            self.db_params = None
        self.url = url
        self.credentials = credentials
        self.timeout = timeout

    def connect(self) -> Connection:
        return self.client(self.url, db_params=self.db_params, credentials=self.credentials, timeout=self.timeout)
