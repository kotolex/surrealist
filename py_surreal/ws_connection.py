import urllib.parse
from typing import Optional, Tuple, Dict, Union, List, Callable, Any

from py_surreal.connection import Connection, connected
from py_surreal.errors import (SurrealConnectionError, WebSocketConnectionError, ConnectionParametersError,
                               WebSocketConnectionClosed)
from py_surreal.utils import DEFAULT_TIMEOUT, SurrealResult
from py_surreal.ws_client import WebSocketClient


class WebSocketConnection(Connection):
    def __init__(self, url: str, db_params: Optional[Dict] = None, credentials: Optional[Tuple[str, str]] = None,
                 timeout: int = DEFAULT_TIMEOUT):
        super().__init__(db_params, credentials, timeout)
        base_url = urllib.parse.urlparse(url.lower())
        self.params = {}
        if db_params:
            if "NS" in db_params:
                self.params["NS"] = db_params["NS"]
            if "DB" in db_params:
                self.params["DB"] = db_params["DB"]
            if not self.params:
                raise ConnectionParametersError("Connection parameters namespace and database required")
        if base_url.scheme in ("ws", "wss"):
            self._base_url = base_url
        if base_url.scheme in ("http", "https"):
            self._base_url = f"{base_url.scheme.replace('http', 'ws')}://{base_url.netloc}/rpc"
        try:
            self.client = WebSocketClient(self._base_url, timeout)
        except TimeoutError:
            raise SurrealConnectionError(f"Cant connect to {self._base_url} in {timeout} seconds.\n"
                                         f"Is your SurrealDB started and work on that url? "
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")
        except WebSocketConnectionClosed:
            raise SurrealConnectionError(f"Cant connect to {self._base_url}, connection refused.\n"
                                         f"Is your SurrealDB started and work on that url? "
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")
        self._use_or_sign_on_params(credentials)
        self.connected = True

    def _use_or_sign_on_params(self, credentials):
        if self.params:
            if credentials:
                self._user, self._pass = self.credentials
                signin_result = self.signin(self._user, self._pass, *list(self.params.values()))
                if signin_result.is_error():
                    raise WebSocketConnectionError(f"Error on connecting to '{self._base_url}'.\nInfo: {signin_result}")
            else:
                use_result = self.use(*list(self.params.values()))
                if use_result.is_error():
                    raise WebSocketConnectionError(f"Error on use '{self.params}'.\nInfo: {use_result}")
        else:
            if credentials:
                self._user, self._pass = self.credentials
                signin_result = self.signin(self._user, self._pass)
                if signin_result.is_error():
                    raise WebSocketConnectionError(f"Error on connecting to '{self._base_url}'.\nInfo: {signin_result}")

    @connected
    def use(self, namespace: str, database: str) -> SurrealResult:
        data = {"method": "use", "params": [namespace, database]}
        return self._run(data)

    @connected
    def info(self) -> SurrealResult:
        data = {"method": "info"}
        return self._run(data)

    @connected
    def signin(self, user: str, password: str, namespace: Optional[str] = None, database: Optional[str] = None,
               scope: Optional[str] = None) -> SurrealResult:
        params = {"user": user, "pass": password}
        if namespace is not None:
            params["NS"] = namespace
        if database is not None:
            params["DB"] = database
        if scope is not None:
            params["SC"] = scope
        data = {"method": "signin", "params": [params]}
        return self._run(data)

    @connected
    def signup(self, namespace: str, database: str, scope: str, params: Optional[Dict] = None) -> SurrealResult:
        params = params or {}
        params = {"NS": namespace, "DB": database, "SC": scope, **params}
        data = {"method": "signup", "params": [params]}
        return self._run(data)

    @connected
    def authenticate(self, token: str) -> SurrealResult:
        data = {"method": "authenticate", "params": [token]}
        return self._run(data)

    @connected
    def invalidate(self) -> SurrealResult:
        data = {"method": "invalidate"}
        return self._run(data)

    @connected
    def let(self, name: str, value) -> SurrealResult:
        data = {"method": "let", "params": [name, value]}
        return self._run(data)

    @connected
    def unset(self, name: str) -> SurrealResult:
        data = {"method": "unset", "params": [name]}
        return self._run(data)

    def live(self, table_name: str, callback: Callable[[Dict], Any], need_diff: bool = False) -> SurrealResult:
        params = [table_name]
        if need_diff:
            params.append(True)
        data = {"method": "live", "params": params}
        return self._run(data, callback)

    @connected
    def kill(self, live_query_id: str) -> SurrealResult:
        data = {"method": "kill", "params": [live_query_id]}
        return self._run(data)

    @connected
    def query(self, query: str, variables: Optional[Dict] = None) -> SurrealResult:
        params = [query]
        if variables is not None:
            params.append(variables)
        data = {"method": "query", "params": params}
        return self._run(data)

    @connected
    def select(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        data = {"method": "select", "params": [table_name]}
        return self._run(data)

    @connected
    def create(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        if record_id is not None:
            data["id"] = record_id
        data = {"method": "create", "params": [table_name, data]}
        return self._run(data)

    @connected
    def insert(self, table_name: str, data: Dict) -> SurrealResult:
        data = {"method": "insert", "params": [table_name, data]}
        return self._run(data)

    @connected
    def update(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        data = {"method": "update", "params": [table_name, data]}
        return self._run(data)

    @connected
    def merge(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        data = {"method": "merge", "params": [table_name, data]}
        return self._run(data)

    @connected
    def patch(self, table_name: str, data: Union[Dict, List], record_id: Optional[str] = None,
              return_diff: bool = False) -> SurrealResult:
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        params = [table_name, data]
        if return_diff:
            params.append(return_diff)
        data = {"method": "patch", "params": params}
        return self._run(data)

    @connected
    def delete(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        data = {"method": "delete", "params": [table_name]}
        return self._run(data)

    def close(self):
        super().close()
        if self.is_connected():
            self.client.close()

    def is_connected(self) -> bool:
        return self.client.connected

    def _run(self, data, callback: Callable = None):
        return self.client.send(data, callback)
