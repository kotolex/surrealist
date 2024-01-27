from typing import Tuple

from py_surreal.http_connection import HttpConnection
from py_surreal.ws_client import WebSocketClient


class Surreal:
    def __init__(self, url: str, namespace: str, database: str, credentials: Tuple[str, str] = None,
                 use_http: bool = True, timeout: int = 5):
        self.client = HttpConnection if use_http else WebSocketClient
        self.namespace = namespace
        self.database = database
        self.url = url
        self.credentials = credentials
        self.timeout = timeout
        self.connection = None

    def connect(self):
        self.connection = self.client(self.url, namespace=self.namespace, database=self.database,
                                      credentials=self.credentials, timeout=self.timeout)
        return self.connection

    def close(self):
        self.connection.close()
        del self.connection
