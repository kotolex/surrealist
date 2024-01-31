from typing import Tuple, Optional
from logging import getLogger, ERROR, DEBUG, basicConfig, INFO

from py_surreal.connections.connection import Connection
from py_surreal.connections.http_connection import HttpConnection
from py_surreal.utils import DEFAULT_TIMEOUT, _set_length, DATA_LENGTH_FOR_LOGS
from py_surreal.connections.ws_connection import WebSocketConnection

FORMAT = '%(asctime)s : %(threadName)s : %(name)s : %(levelname)s : %(message)s'
basicConfig(level=ERROR, format=FORMAT)
logger = getLogger()


class Surreal:
    """
    Represents the SurrealDB server, all connections should be established via this class. By default, connection will
    be via websocket client (recommended). Can use log level to monitor all actions
    """

    def __init__(self, url: str, namespace: Optional[str] = None, database: Optional[str] = None,
                 credentials: Tuple[str, str] = None, use_http: bool = False, timeout: int = DEFAULT_TIMEOUT,
                 log_level: str = "ERROR"):
        """
        Initiating all parameters for connection


        :param url: db url, for local database http://127.0.0.1:8000/
        :param namespace: namespace to use
        :param database: database to use, make sure to use both namespace and database or none of them
        :param credentials: pair of user and password, for example ("root", "root")
        :param use_http: boolean flag of using http-client. Will use websocket-client if False.
        It is strongly recommended to use websocket client as it is more powerful.
        :param timeout: connection timeout
        :param log_level: level of logging for debug purposes, one of (DEBUG, INFO, ERROR) allowed, where ERROR
        is default, INFO - to see only operations, DEBUG - to see all, including http/websockets requests
        """
        self.log_level = log_level
        self.log_data_length = DATA_LENGTH_FOR_LOGS
        self.set_log_level(log_level)
        self.client = HttpConnection if use_http else WebSocketConnection
        self.db_params = {}
        if namespace:
            self.db_params["NS"] = namespace
        if database:
            self.db_params["DB"] = database
        if not self.db_params:
            self.db_params = None
        self.url = url if url.endswith("/") else f"{url}/"
        self.credentials = credentials
        self.timeout = timeout

    def set_log_length_for_data(self, length: int):
        _set_length(length)
        self.log_data_length = length

    def set_log_level(self, level: str):
        if level not in ("DEBUG", "INFO", "ERROR"):
            raise ValueError("Log level shoud be one of (DEBUG, INFO, ERROR), where ERROR is default")
        levels = {"DEBUG": DEBUG, "INFO": INFO, "ERROR": ERROR}
        new_level = levels[level]
        logger.setLevel(new_level)
        logger.debug("Log level switch to %s", level)
        self.log_level = level

    def connect(self) -> Connection:
        """
        Actually connects to database via chosen client

        :return: connection object to use
        """
        return self.client(self.url, db_params=self.db_params, credentials=self.credentials, timeout=self.timeout)
