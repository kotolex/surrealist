from typing import Tuple, Optional

from py_surreal.http_connection import HttpConnection
from py_surreal.ws_client import WebSocketClient


class Surreal:
    def __init__(self, url: str, namespace: Optional[str]=None, database: Optional[str]=None,
                 credentials: Tuple[str, str] = None,
                 use_http: bool = True, timeout: int = 5):
        self.client = HttpConnection if use_http else WebSocketClient
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
        self.connection = None

    def connect(self):
        self.connection = self.client(self.url, db_params=self.db_params, credentials=self.credentials,
                                      timeout=self.timeout)
        return self.connection

    def close(self):
        self.connection.close()
        del self.connection
