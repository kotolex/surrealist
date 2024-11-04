from logging import getLogger
from pathlib import Path
from typing import Tuple, Dict, Optional, Union, Any, BinaryIO

from surrealist.clients.http_client import HttpClient
from surrealist.connections.connection import Connection, connected
from surrealist.enums import Transport
from surrealist.errors import (CompatibilityError, HttpConnectionError, HttpClientError, SurrealConnectionError)
from surrealist.result import SurrealResult, to_result
from surrealist.utils import (ENCODING, DEFAULT_TIMEOUT, crop_data, HTTP_OK, NS, DB, AC)

logger = getLogger("surrealist.connections.http")


class HttpConnection(Connection):
    """
    Represents http transport and abilities to work with SurrealDb. It is not a recommended connection, use websocket in 
    any doubt; this connection exists for compatibility reasons. Some features as live query are not possible on this
    transport, but import and export are.

    Refer to surrealist documentation: https://github.com/kotolex/surrealist?tab=readme-ov-file#transports
    Refer to: https://docs.surrealdb.com/docs/integration/http

    Each method call creates a new http short-live connection.

    On creating, this object tries to create a connection with specified data and will raise exception on fail.
    """

    def __init__(self, url: str, db_params: Optional[Dict] = None, credentials: Tuple[str, str] = None,
                 timeout: int = DEFAULT_TIMEOUT):
        super().__init__(db_params, credentials, timeout)
        self._url = url
        self._http_client = HttpClient(url, headers=db_params, credentials=credentials, timeout=timeout)
        self._sign(credentials, db_params, url)
        self._connected = True
        masked_creds = None if not credentials else (credentials[0], "******")
        logger.info("Connected to %s, params: %s, credentials: %s, timeout: %s", url, db_params, masked_creds, timeout)

    def _use_rpc(self, data) -> SurrealResult:
        _, text = self._rpc(data)
        return to_result(text)

    def _sign(self, credentials, db_params, url):
        user, password, ns, db, ac = None, None, None, None, None
        if credentials:
            user, password = credentials
        if db_params:
            ns = db_params.get(NS)
            db = db_params.get(DB)
            ac = db_params.get(AC)
        try:
            result = self._signin(user, password, namespace=ns, database=db, access=ac)
            if result.is_error():
                logger.error("Cant sign in to %s with given credentials", url)
                raise SurrealConnectionError(f"Cant sign in to {url} with given credentials\n"
                                             f"Info: {result.additional_info}\n")
            self._http_client.set_token(result.result)
            self._token = result.result
        except HttpClientError:
            logger.error("Cant connect to %s", url)
            raise SurrealConnectionError(f"Cant connect to {url}\n"
                                         f"Is your SurrealDB started and work on that url? "
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")

    def transport(self) -> Transport:
        return Transport.HTTP

    @connected
    def import_data(self, path: Union[str, Path]) -> SurrealResult:
        """
        This method imports a SurrealQL script file into a local or remote SurrealDB database server.

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#import
        https://docs.surrealdb.com/docs/cli/import

        Refer to: https://docs.surrealdb.com/docs/integration/http#import
        :param path: path to file for import 
        :return: result of request
        """
        with open(path, 'rb') as file:
            logger.info("Operation: IMPORT. Path: %s", crop_data(str(path)))
            _, text = self._simple_request("POST", "import", file, type_of_content="FILE")
        return to_result(text)

    @connected
    def export(self) -> str:
        """
        This method exports all data for a specific namespace and database.

        Refer to: https://docs.surrealdb.com/docs/integration/http#export

        Refer to: https://docs.surrealdb.com/docs/cli/export/


        :return: text of the exported file to use or save
        """
        text = raise_if_not_http_ok(self._simple_get("export"))
        return text

    @connected
    def ml_import(self, path: Union[str, Path]) -> SurrealResult:
        """
        This method imports a SurrealQL ML file into a local or remote SurrealDB database server.

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#ml-import
        https://docs.surrealdb.com/docs/cli/ml/import

        :param path: path to file for import
        :return: result of request
        """
        with open(path, 'rb') as file:
            logger.info("Operation: ML IMPORT. Path: %s", crop_data(str(path)))
            _, text = self._simple_request("POST", "ml/import", file.read().decode(ENCODING), type_of_content="STR")
        return to_result(text)

    @connected
    def ml_export(self, name: str, version: str) -> str:
        """
        This method exports a SurrealML machine learning model from a specific namespace and database. As machine
        learning files can be large, the endpoint outputs a chunked HTTP response.

        Refer to: https://docs.surrealdb.com/docs/integration/http#ml-export

        Refer to: https://docs.surrealdb.com/docs/cli/ml/export

        Example:
        http_connection.ml_export("prediction", "1.0.0")

        :param name: name of ML model
        :param version: version of ML model
        :return:  text of the exported file to use or save
        """
        logger.info("Operation: ML EXPORT. Name %s Version %s", name, version)
        text = raise_if_not_http_ok(self._simple_get(f"ml/export/{name}/{version}"))
        return text

    @connected
    def use(self, namespace: str, database: Optional[str] = None) -> None:
        """
        This method specifies the namespace and optional database for the current connection. For http-connection, it
        actually means using headers "surreal-ns" and "surreal-db", no checks for namespace or database to be existent
        will be made

        Example:
        http_connection.use("test", "test") # use test namespace and test database for this connection

        :param namespace: name of the namespace to use
        :param database: name of the database to use (optional)
        :return: None
        """
        logger.info("Operation: USE. Namespace: %s, database %s", crop_data(namespace), crop_data(database or "None"))
        self._db_params = {NS: namespace}
        if database:
            self._db_params[DB] = database
        self._http_client.set_db_params(self._db_params)

    def live(self, table_name, callback, return_diff: bool = False):
        """
        Http transport cannot use live queries, you should use websocket transport for that

        :raise CompatibilityError: on any use
        """
        message = "Http transport cannot use live queries, use websockets instead"
        logger.error(message)
        raise CompatibilityError(message)

    def custom_live(self, custom_query, callback):
        """
        Http transport cannot use live queries, you should use websocket transport for that

        :raise CompatibilityError: on any use
        """
        message = "Http transport cannot use custom live queries, use websockets instead"
        logger.error(message)
        raise CompatibilityError(message)

    def kill(self, live_query_id: str):
        """
        Http transport cannot use KILL operation, you should use websocket transport for that

        :raise CompatibilityError: on any use
        """
        message = "Http transport cannot use kill, use websockets instead"
        logger.error(message)
        raise CompatibilityError(message)

    def let(self, name: str, value: Any):
        """
        Http transport cannot use LET method, you should use websocket for that

        :raise CompatibilityError: on any use
        """
        message = "Http transport cannot use LET method, you should use websocket for that"
        logger.error(message)
        raise CompatibilityError(message)

    def unset(self, name: str):
        """
        Http transport cannot use UNSET method, you should use websocket for that

        :raise CompatibilityError: on any use
        """
        message = "Http transport cannot use UNSET method, you should use websocket for that"
        logger.error(message)
        raise CompatibilityError(message)

    def _simple_get(self, endpoint: str) -> Tuple[int, str]:
        with self._http_client.get(endpoint) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            _body = "is empty" if not text else crop_data(text)
            logger.info("Response from /%s, status_code: %s, body: %s", endpoint, status, _body)
            return status, text

    def _rpc(self, data: Union[Dict, str]) -> Tuple[int, str]:
        return self._simple_request("POST", "rpc", data)

    def _simple_request(self, method, endpoint: str, data: Union[Dict, str, BinaryIO],
                        type_of_content: str = "JSON") -> Tuple[int, str]:
        with self._http_client.request(method, data, endpoint, type_of_content=type_of_content) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            if type_of_content == "FILE":
                data.close()
            _body = "is empty" if not text else crop_data(text)
            logger.info("Response from /%s, status_code: %s, body %s", endpoint, status, _body)
        return status, text


def raise_if_not_http_ok(result: Tuple[int, str]) -> str:
    """
    Helper for methods which need only success responses

    :param result: pair of status_code and text from the http response
    :return: text of the response if status is 200, else raise
    :raise HttpConnectionError: if status code is not 200
    """
    status, text = result
    if status != HTTP_OK:
        logger.error("Status code is %s, check your data! Info %s", status, text)
        text = text.replace(',', ',\n')
        raise HttpConnectionError(f"Status code is {status}, check your data! Info:\n{text}")
    return text
