import urllib.parse
from logging import getLogger, ERROR, DEBUG, basicConfig, INFO
from typing import Tuple, Optional

from surrealist.clients import HttpClient
from surrealist.connections.connection import Connection
from surrealist.connections.http_connection import HttpConnection
from surrealist.connections.ws_connection import WebSocketConnection
from surrealist.errors import HttpClientError, SurrealConnectionError
from surrealist.utils import DEFAULT_TIMEOUT, _set_length, DATA_LENGTH_FOR_LOGS, ENCODING, OK, HTTP_OK

FORMAT = '%(asctime)s : %(threadName)s : %(name)s : %(levelname)s : %(message)s'
basicConfig(level=ERROR, format=FORMAT)
logger = getLogger()


class Surreal:
    """
    Represents the SurrealDB client, all connections should be established via this class. By default, connection will
    use websocket transport (recommended). This class uses log level to monitor all actions, can check a version and
    state of the SurrealDb server.
    If you have only one SurrealDB server, you need exactly one object of this class!

    Refer to: https://github.com/kotolex/surrealist?tab=readme-ov-file#connect-to-surrealdb
    """

    def __init__(self, url: str, namespace: Optional[str] = None, database: Optional[str] = None,
                 credentials: Tuple[str, str] = None, use_http: bool = False, timeout: int = DEFAULT_TIMEOUT,
                 log_level: str = "ERROR"):
        """
        Initiating all parameters for connection, this method does not check or validates anything by itself, just save
        data for future use. To make sure your url is valid and accessible - use **is_ready** method of Surreal object.

        About log_level and debug mode: https://github.com/kotolex/surrealist?tab=readme-ov-file#debug-mode

        :param url: database url, for local database http://127.0.0.1:8000/
        :param namespace: namespace to use
        :param database: a database to use, make sure to use both namespace and database or none of them
        :param credentials: pair of user and password, for example ("root", "root")
        :param use_http: boolean flag of using http transport. Will use websocket-client if False.
        It is strongly recommended to use websocket transport as it is more powerful.
        :param timeout: connection timeout in seconds
        :param log_level: level of logging for debug purposes, one of (DEBUG, INFO, ERROR) allowed, where ERROR
        is default, INFO - to see only operations, DEBUG - to see all, including transport requests/responses
        """
        self._log_level = log_level
        self._log_data_length = DATA_LENGTH_FOR_LOGS
        self.set_log_level(log_level)
        self._client = HttpConnection if use_http else WebSocketConnection
        self.db_params = {}
        if namespace:
            self.db_params["NS"] = namespace
        if database:
            self.db_params["DB"] = database
        if not self.db_params:
            self.db_params = None
        self._url, self._possible_url, self._is_http_url = url, url, True
        self.set_url(url)
        self.credentials = credentials
        self.timeout = timeout

    def set_url(self, url: str):
        """
        Setting base and predicted urls to work with. If url starts with http, then both url and predicted url will be
        the same. If url in ws(s) format, method will try to create a predicted url based on it.

        For example for wss://127.0.0.1:9000/some/rps predicted http url will be https://127.0.0.1:9000/

        :param url: url of the SurrealDb server
        """
        self._url = url
        self._is_http_url = url.startswith("http")
        # we should change only http endpoints
        if self._is_http_url and not url.endswith("/"):
            self._url = f"{url}/"
        self._possible_url = self._url
        # we can try to predict http url when wss url was specified
        if not self._is_http_url:
            _url = urllib.parse.urlparse(url.lower())
            self._possible_url = f"{_url.scheme.replace('ws', 'http')}://{_url.netloc}/"
            logger.info("Predicted url is: %s", self._possible_url)

    def set_log_length_for_data(self, length: int):
        """
        Setting maximum string length for object(json) in logs, it is not always desirable to see all data that comes
        in and out in logs, because some objects(jsons) can be very large, but for debug purposes you can increase it
        to see the full picture. However, it is not recommended to set length too small for speed reasons, or set it
        too large, except when you are in the debug mode. Default is 300 chars, which is enough for standard
        SurrealDB messages.

        :param length: new maximum length for one string in logs
        """
        _set_length(length)
        self._log_data_length = length

    def set_log_level(self, level: str):
        """
        Setting log level for all underlying connections of that Surreal object.
        There are 3 options:
        - ERROR which is default and recommended for most situations, you will see only errors
        - INFO which is show all information about connection operations
        - DEBUG, when you will see all logs, including requests and responses via transport level. This level is very
        useful for debugging and finding some useful information in logs

        Refer to: https://github.com/kotolex/surrealist?tab=readme-ov-file#debug-mode

        Notice: authorization params and passwords can't be seen in logs at any time, if you see it, please report
        an issue

        Notice: you will see logs of this library(in and out), but not SurrealDb server logs

        :param level: one of "DEBUG", "INFO", "ERROR"
        """
        if level not in ("DEBUG", "INFO", "ERROR"):
            raise ValueError("Log level should be one of (DEBUG, INFO, ERROR), where ERROR is default")
        levels = {"DEBUG": DEBUG, "INFO": INFO, "ERROR": ERROR}
        new_level = levels[level]
        logger.setLevel(new_level)
        logger.debug("Log level switch to %s", level)
        self._log_level = level

    def connect(self) -> Connection:
        """
        Actually connects to a database via chosen transport (websocket or http), uses specified namespace, database and
        credentials parameters, so can raise exception if connect will fail. If this method succeeded, you are
        ready to go with SurrealDB

        :return: connection object to work with SurrealDB
        :raise SurrealConnectionError: if cant connect with specified parameters
        """
        return self._client(self._url, db_params=self.db_params, credentials=self.credentials, timeout=self.timeout)

    def is_ready(self) -> bool:
        """
        Checks that SurrealDB server is up and running. Under the hood it calls **health** and **status** methods to
        make sure database web server is running, database server and storage engine are running.

        Notice: Method uses the specified url, but not namespace, database or credentials, so success of this method
        (return True) should not be interpreted as authorization success

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#status

        https://docs.surrealdb.com/docs/integration/http#health

        :return: True if you can use this server, False otherwise
        :raise SurrealConnectionError: if cant connect to http-endpoint
        """
        return self.status() == OK and self.health() == OK

    def status(self) -> str:
        """
        Checks that SurrealDB web server is running.

        Refer to: https://docs.surrealdb.com/docs/integration/http#status

        :return: "OK" if everything is ok, or error text
        :raise SurrealConnectionError: if cant connect to http-endpoint
        """
        code, text = self.__get("status")
        return OK if code == HTTP_OK else text

    def health(self) -> str:
        """
        Checks that SurrealDB server and storage engine are running.

        Refer to: https://docs.surrealdb.com/docs/integration/http#health

        :return: "OK" if everything is ok, or error text
        :raise SurrealConnectionError: if cant connect to http-endpoint
        """
        code, text = self.__get("health")
        return OK if code == HTTP_OK else text

    def version(self) -> str:
        """
        Get SurrealDB version

        Refer to: https://docs.surrealdb.com/docs/integration/http#version

        :return: version, for example, "surrealdb-1.1.1"
        :raise SurrealConnectionError: if cant connect to http-endpoint
        """
        return self.__get("version")[1]

    def __get(self, endpoint: str) -> Tuple[int, str]:
        try:
            with HttpClient(self._possible_url, timeout=DEFAULT_TIMEOUT).get(endpoint) as resp:
                status, text = resp.status, resp.read().decode(ENCODING)
                body = "is empty" if not text else text
                logger.info("Response from /%s, status_code: %s, body: %s", endpoint, status, body)
                if status != HTTP_OK:
                    logger.error("Status is %s for predicted http %s", status, self._possible_url)
                return status, text
        except HttpClientError:
            logger.error("Cant connect to %s", self._possible_url)
            raise SurrealConnectionError(f"Cant connect to {self._possible_url}{endpoint}\n"
                                         f"Is your SurrealDB started and work on {self._possible_url} ?\n"
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")
