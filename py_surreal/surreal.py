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


if __name__ == '__main__':
    sur = Surreal("http://127.0.0.1:8000/", "test", "test", credentials=('root', 'root'), use_http=True)
    sur = sur.connect()
    print(sur.is_ready())
    print(sur.status())
    print(sur.health())
    print(sur.version())
    print(sur.select("article", "fbk43xn5vdb026hdscnz"))
    print(sur.select("article"))
