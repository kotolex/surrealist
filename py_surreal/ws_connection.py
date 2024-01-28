import urllib.parse
from typing import Optional, Tuple, Dict

from py_surreal.errors import (SurrealConnectionError, WebSocketConnectionError, ConnectionParametersError,
                               WebSocketConnectionClosed)
from py_surreal.ws_client import WebSocketClient
from py_surreal.utils import DEFAULT_TIMEOUT, SurrealResult


class WebSocketConnection:
    def __init__(self, base_url: str, db_params: Optional[Dict] = None, credentials: Optional[Tuple[str, str]] = None,
                 timeout: int = DEFAULT_TIMEOUT):
        base_url = urllib.parse.urlparse(base_url.lower())
        self.credentials = credentials
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

    def _use_or_sign_on_params(self, credentials):
        if self.params:
            if credentials:
                self._user, self._pass = self.credentials
                signin_result = self.signin(self._user, self._pass, self.params)
                if signin_result.is_error():
                    raise WebSocketConnectionError(f"Error on connecting to '{self._base_url}'.\nInfo: {signin_result}")
            else:
                use_result = self.use(self.params)
                if use_result.is_error():
                    raise WebSocketConnectionError(f"Error on use '{self.params}'.\nInfo: {use_result}")
        else:
            if credentials:
                self._user, self._pass = self.credentials
                signin_result = self.signin(self._user, self._pass)
                if signin_result.is_error():
                    raise WebSocketConnectionError(f"Error on connecting to '{self._base_url}'.\nInfo: {signin_result}")

    def use(self, namespace: str, database: str) -> SurrealResult:
        data = {"method": "use", "params": [namespace, database]}
        return self._run(data)

    def info(self) -> SurrealResult:
        data = {"method": "info"}
        return self._run(data)

    def signin(self, user: str, password: str, params: Optional[Dict] = None) -> SurrealResult:
        data = {"method": "signin", "params": [{"user": user, "pass": password}]}
        if params:
            result = {}
            if "NS" in params:
                result = {"NS": params["NS"]}
            if "DB" in params:
                result["DB"] = params["DB"]
            data['params'][0] = {**result, **data['params'][0]}
        return self._run(data)

    def all_records_at(self, name: str):
        data = {"method": "select", "params": [name]}
        return self._run(data)

    def close(self):
        if self.client and self.client.connected:
            self.client.close()

    def is_connected(self) -> bool:
        return self.client.connected

    def _run(self, data):
        return self.client.send(data)

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()
