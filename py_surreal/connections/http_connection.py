from logging import getLogger
from pathlib import Path
from typing import Tuple, Dict, Optional, Union

from py_surreal.connections.connection import Connection, connected
from py_surreal.errors import SurrealConnectionError, HttpClientError, CompatibilityError, HttpConnectionError
from py_surreal.clients.http_client import HttpClient
from py_surreal.utils import (ENCODING, to_result, SurrealResult, OK, DEFAULT_TIMEOUT, crop_data, mask_pass)

logger = getLogger("http_connection")


class HttpConnection(Connection):
    def __init__(self, url: str, db_params: Optional[Dict] = None, credentials: Tuple[str, str] = None,
                 timeout: int = DEFAULT_TIMEOUT):
        super().__init__(db_params, credentials, timeout)
        self.http_client = HttpClient(url, headers=db_params, credentials=credentials, timeout=timeout)
        try:
            is_ready = self.is_ready()
        except HttpClientError:
            is_ready = False
        if not is_ready:
            logger.error("Cant connect to %s OR /status and /health are not OK", url)
            raise SurrealConnectionError(f"Cant connect to {url} OR /status and /health are not OK.\n"
                                         f"Is your SurrealDB started and work on that url? "
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")
        self.connected = True
        masked_creds = None if not credentials else (credentials[0], "******")
        logger.info("Connected to %s, params: %s, credentials: %s, timeout: %s", url, db_params, masked_creds, timeout)

    def is_ready(self) -> bool:
        return self.status() == OK and self.health() == OK

    def status(self) -> str:
        code, text = self._simple_get("status")
        return OK if code == 200 else text

    def health(self) -> str:
        code, text = self._simple_get("health")
        return OK if code == 200 else text

    def version(self) -> str:
        return self._simple_get("version")[1]

    @connected
    def export(self) -> str:
        _, text = raise_if_not_http_ok(self._simple_get("export"))
        return text

    @connected
    def select(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        logger.info("Operation: SELECT. Path: %s", crop_data(url))
        _, text = raise_if_not_http_ok(self._simple_get(url))
        return to_result(text)

    @connected
    def ml_export(self, name: str, version: str) -> str:
        logger.info("Operation: ML EXPORT. Name %s Version %s", name, version)
        _, text = raise_if_not_http_ok(self._simple_get(f"ml/export/{name}/{version}"))
        return text

    @connected
    def signin(self, user: str, password: str, namespace: Optional[str] = None,
               database: Optional[str] = None, scope: Optional[str] = None) -> SurrealResult:
        opts = {"user": user, "pass": password}
        if namespace:
            opts['ns'] = namespace
        if database:
            opts['db'] = database
        if scope:
            opts['scope'] = scope
        logger.info("Operation: SIGNIN. Data: %s", crop_data(mask_pass(str(opts))))
        _, text = raise_if_not_http_ok(self._simple_request("POST", "signin", opts))
        return to_result(text)

    @connected
    def signup(self, namespace: str, database: str, scope: str, params: Optional[Dict] = None) -> SurrealResult:
        params = params or {}
        body = {"ns": namespace, "db": database, "sc": scope, **params}
        logger.info("Operation: SIGNUP. Data: %s", crop_data(mask_pass(str(body))))
        _, text = raise_if_not_http_ok(self._simple_request("POST", "signup", body))
        return to_result(text)

    @connected
    def create(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        logger.info("Operation: CREATE. Path: %s, data: %s", crop_data(url), crop_data(str(data)))
        _, text = raise_if_not_http_ok(self._simple_request("POST", url, data))
        return to_result(text)

    @connected
    def update(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        logger.info("Operation: UPDATE. Path: %s, data: %s", crop_data(url), crop_data(str(data)))
        _, text = raise_if_not_http_ok(self._simple_request("PUT", url, data))
        return to_result(text)

    @connected
    def delete(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        logger.info("Operation: DELETE. Path: %s", crop_data(url))
        _, text = raise_if_not_http_ok(self._simple_request("DELETE", url, {}))
        return to_result(text)

    @connected
    def patch(self, table_name: str, data: Dict, record_id: Optional[str] = None,
              return_diff: bool = False) -> SurrealResult:
        if return_diff:
            logger.warning(
                "DIFF option is not used on PATCH with http-connection, use QUERY if you need DIFF or switch to websocket connection")
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        logger.info("Operation: PATCH. Path: %s, data: %s", crop_data(url), crop_data(str(data)))
        _, text = raise_if_not_http_ok(self._simple_request("PATCH", url, data))
        return to_result(text)

    @connected
    def query(self, query: str, variables: Optional[Dict] = None) -> SurrealResult:
        if variables is not None:
            logger.warning(
                "Variables parameter cant be used on QUERY with http-connection, embed them into your query or switch to websocket connection")
        logger.info("Operation: QUERY. Query: %s", crop_data(query))
        _, text = raise_if_not_http_ok(self._simple_request("POST", "sql", query, not_json=True))
        return to_result(text)

    @connected
    def import_data(self, path: Union[str, Path]) -> SurrealResult:
        with open(path, 'rb') as file:
            logger.info("Operation: IMPORT. Path: %s", crop_data(str(path)))
            _, text = raise_if_not_http_ok(self._simple_request("POST", "import", file.read().decode(ENCODING),
                                                                not_json=True))
        return to_result(text)

    @connected
    def ml_import(self, path: Union[str, Path]) -> SurrealResult:
        with open(path, 'rb') as file:
            logger.info("Operation: ML IMPORT. Path: %s", crop_data(str(path)))
            _, text = raise_if_not_http_ok(self._simple_request("POST", "ml/import", file.read().decode(ENCODING),
                                                                not_json=True))
        return to_result(text)

    def live(self, table_name, callback, need_diff: bool = False) -> SurrealResult:
        # TODO link here
        raise CompatibilityError("Http-client cant use live queries, more on that: url")

    def kill(self, live_query_id: str) -> SurrealResult:
        # TODO link here
        raise CompatibilityError("Http-client cant kill queries, more on that: url")

    def _simple_get(self, endpoint: str) -> Tuple[int, str]:
        with self.http_client.get(endpoint) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            _body = "is empty" if not text else crop_data(text)
            logger.info("Response from /%s, status_code: %s, body: %s", endpoint, status, _body)
            return status, text

    def _simple_request(self, method, endpoint: str, data: Union[Dict, str], not_json: bool = False) -> Tuple[int, str]:
        with self.http_client.request(method, data, endpoint, not_json) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            _body = "is empty" if not text else crop_data(text)
            logger.info("Response from /%s, status_code: %s, body %s", endpoint, status, _body)
            return status, text


def raise_if_not_http_ok(result: Tuple[int, str]):
    status, text = result
    if status != 200:
        logger.error("Status code is %s, check your data! Info %s", status, text)
        text = text.replace(',', ',\n')
        raise HttpConnectionError(f"Status code is {status}, check your data! Info:\n{text}")
    return result
