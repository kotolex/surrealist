from typing import Tuple, Dict, Optional, Union

from py_surreal.utils import (ENCODING, to_result, SurrealResult, raise_if_not_http_ok, OK, DEFAULT_TIMEOUT)
from py_surreal.errors import SurrealConnectionError, HttpClientError, CompatibilityError
from py_surreal.http_client import HttpClient
from py_surreal.connection import Connection, connected


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
            raise SurrealConnectionError(f"Cant connect to {url} OR /status and /health are not OK.\n"
                                         f"Is your SurrealDB started and work on that url? "
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")
        self.connected = True

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
        _, text = raise_if_not_http_ok(self._simple_get(url))
        return to_result(text)

    @connected
    def ml_export(self, name: str, version: str) -> str:
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
        _, text = raise_if_not_http_ok(self._simple_request("POST", "signin", opts))
        return to_result(text)

    @connected
    def signup(self, namespace: str, database: str, scope: str, params: Optional[Dict] = None) -> SurrealResult:
        params = params or {}
        body = {"ns": namespace, "db": database, "sc": scope, **params}
        _, text = raise_if_not_http_ok(self._simple_request("POST", "signup", body))
        return to_result(text)

    @connected
    def create(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        _, text = raise_if_not_http_ok(self._simple_request("POST", url, data))
        return to_result(text)

    @connected
    def update(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        _, text = raise_if_not_http_ok(self._simple_request("PUT", url, data))
        return to_result(text)

    @connected
    def delete(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        _, text = raise_if_not_http_ok(self._simple_request("DELETE", url, {}))
        return to_result(text)

    @connected
    def patch(self, table_name: str, data: Dict, record_id: Optional[str] = None,
              return_diff: bool = False) -> SurrealResult:
        # return diff is not working here
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        _, text = raise_if_not_http_ok(self._simple_request("PATCH", url, data))
        return to_result(text)

    @connected
    def query(self, query: str, variables: Optional[Dict] = None) -> SurrealResult:
        if variables is not None:
            raise CompatibilityError("Http-client cant use additional variables in /query")
        _, text = raise_if_not_http_ok(self._simple_request("POST", "sql", query, not_json=True))
        return to_result(text)

    @connected
    def import_data(self, path) -> SurrealResult:
        with open(path, 'rb') as file:
            _, text = raise_if_not_http_ok(self._simple_request("POST", "import", file.read().decode(ENCODING),
                                                                not_json=True))
        return to_result(text)

    @connected
    def ml_import(self, path) -> SurrealResult:
        with open(path, 'rb') as file:
            _, text = raise_if_not_http_ok(self._simple_request("POST", "ml/import", file.read().decode(ENCODING),
                                                                not_json=True))
        return to_result(text)

    def _simple_get(self, endpoint: str) -> Tuple[int, str]:
        with self.http_client.get(endpoint) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            return status, text

    def _simple_request(self, method, endpoint: str, data: Union[Dict, str], not_json: bool = False) -> Tuple[int, str]:
        with self.http_client.request(method, data, endpoint, not_json) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            return status, text
