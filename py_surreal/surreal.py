from typing import Tuple, Optional

from py_surreal.connection import Connection
from py_surreal.http_connection import HttpConnection
from py_surreal.utils import DEFAULT_TIMEOUT
from py_surreal.ws_connection import WebSocketConnection


class Surreal:
    """
    Represents the SurrealDB server, all connections should be established via this class. By default connection will
    be via websocket client (recommended)
    """

    def __init__(self, url: str, namespace: Optional[str] = None, database: Optional[str] = None,
                 credentials: Tuple[str, str] = None, use_http: bool = False, timeout: int = DEFAULT_TIMEOUT):
        """
        Initiating all parameters for connection


        :param url: db url, for local database http://127.0.0.1:8000
        :param namespace: namespace to use
        :param database: database to use, make sure to use both namespace and database or none of them
        :param credentials: pair of user and password, for example ("root", "root")
        :param use_http: boolean flag of using http-client. Will use websocket-client if False.
        It is strongly recommended to use websocket client as it is more powerful.
        :param timeout: connection timeout
        """
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
        """
        Actually connects to database via chosen client

        :return: connection object to use
        """
        return self.client(self.url, db_params=self.db_params, credentials=self.credentials, timeout=self.timeout)
