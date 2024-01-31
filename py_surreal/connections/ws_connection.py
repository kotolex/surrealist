import urllib.parse
from logging import getLogger
from typing import Optional, Tuple, Dict, Union, List, Callable, Any

from py_surreal.connections.connection import Connection, connected
from py_surreal.errors import (SurrealConnectionError, WebSocketConnectionError, ConnectionParametersError,
                               WebSocketConnectionClosedError)
from py_surreal.utils import DEFAULT_TIMEOUT, SurrealResult, crop_data, mask_pass
from py_surreal.clients.ws_client import WebSocketClient

logger = getLogger("websocket_connection")


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
                message = "Connection parameters namespace and database required"
                logger.error(message)
                raise ConnectionParametersError(message)
        if base_url.scheme in ("ws", "wss"):
            self._base_url = base_url
        if base_url.scheme in ("http", "https"):
            self._base_url = f"{base_url.scheme.replace('http', 'ws')}://{base_url.netloc}/rpc"
        try:
            self.client = WebSocketClient(self._base_url, timeout)
        except TimeoutError:
            logger.error("Cant connect to %s in %s seconds", self._base_url, self.timeout)
            raise SurrealConnectionError(f"Cant connect to {self._base_url} in {timeout} seconds.\n"
                                         f"Is your SurrealDB started and work on that url? "
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")
        except WebSocketConnectionClosedError:
            logger.error("Cant connect to %s , connection refused", self._base_url)
            raise SurrealConnectionError(f"Cant connect to {self._base_url}, connection refused.\n"
                                         f"Is your SurrealDB started and work on that url? "
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")
        self._use_or_sign_on_params(credentials)
        self.connected = True
        masked_creds = None if not credentials else (credentials[0], "******")
        logger.info("Connected to %s, params: %s, credentials: %s, timeout: %s", self._base_url, db_params,
                    masked_creds, timeout)

    def _use_or_sign_on_params(self, credentials):
        if self.params:
            if credentials:
                self._user, self._pass = self.credentials
                signin_result = self.signin(self._user, self._pass, *list(self.params.values()))
                if signin_result.is_error():
                    logger.error("Error on connecting to %s. Info %s", self._base_url, signin_result)
                    raise WebSocketConnectionError(f"Error on connecting to {self._base_url}.\n"
                                                   f"Info: {signin_result.error}")
            else:
                use_result = self.use(*list(self.params.values()))
                if use_result.is_error():
                    logger.error("Error on use %s. Info %s", self.params, use_result)
                    raise WebSocketConnectionError(f"Error on use '{self.params}'.\nInfo: {use_result.error}")
        else:
            if credentials:
                self._user, self._pass = self.credentials
                signin_result = self.signin(self._user, self._pass)
                if signin_result.is_error():
                    logger.error("Error on connecting to %s. Info %s", self._base_url, signin_result)
                    raise WebSocketConnectionError(f"Error on connecting to '{self._base_url}'.\nInfo: {signin_result}")

    @connected
    def use(self, namespace: str, database: str) -> SurrealResult:
        data = {"method": "use", "params": [namespace, database]}
        logger.info("Operation: USE. Namespace: %s, database %s", crop_data(namespace), crop_data(database))
        return self._run(data)

    @connected
    def info(self) -> SurrealResult:
        data = {"method": "info"}
        logger.info("Operation: INFO")
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
        logger.info("Operation: SIGNIN. Data: %s", crop_data(mask_pass(str(params))))
        return self._run(data)

    @connected
    def signup(self, namespace: str, database: str, scope: str, params: Optional[Dict] = None) -> SurrealResult:
        params = params or {}
        params = {"NS": namespace, "DB": database, "SC": scope, **params}
        data = {"method": "signup", "params": [params]}
        logger.info("Operation: SIGNUP. Data: %s", crop_data(mask_pass(str(params))))
        return self._run(data)

    @connected
    def authenticate(self, token: str) -> SurrealResult:
        data = {"method": "authenticate", "params": [token]}
        logger.info("Operation: AUTHENTICATE. Token: %s", crop_data(token))
        return self._run(data)

    @connected
    def invalidate(self) -> SurrealResult:
        data = {"method": "invalidate"}
        logger.info("Operation: INVALIDATE")
        return self._run(data)

    @connected
    def let(self, name: str, value: Any) -> SurrealResult:
        data = {"method": "let", "params": [name, value]}
        logger.info("Operation: LET. Name: %s, Value: %s", crop_data(name), crop_data(value))
        return self._run(data)

    @connected
    def unset(self, name: str) -> SurrealResult:
        data = {"method": "unset", "params": [name]}
        logger.info("Operation: UNSET. Variable name: %s", crop_data(name))
        return self._run(data)

    def live(self, table_name: str, callback: Callable[[Dict], Any], need_diff: bool = False) -> SurrealResult:
        params = [table_name]
        if need_diff:
            params.append(True)
        data = {"method": "live", "params": params}
        logger.info("Operation: LIVE. Data: %s", crop_data(str(params)))
        return self._run(data, callback)

    @connected
    def kill(self, live_query_id: str) -> SurrealResult:
        data = {"method": "kill", "params": [live_query_id]}
        logger.info("Operation: KILL. Live_id: %s", crop_data(live_query_id))
        return self._run(data)

    @connected
    def query(self, query: str, variables: Optional[Dict] = None) -> SurrealResult:
        params = [query]
        if variables is not None:
            params.append(variables)
        data = {"method": "query", "params": params}
        logger.info("Operation: QUERY. Query: %s, variables: %s", crop_data(query), crop_data(str(variables)))
        return self._run(data)

    @connected
    def select(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        data = {"method": "select", "params": [table_name]}
        logger.info("Operation: SELECT. Path: %s", crop_data(table_name))
        return self._run(data)

    @connected
    def create(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        if record_id is not None:
            data["id"] = record_id
        _data = {"method": "create", "params": [table_name, data]}
        logger.info("Operation: CREATE. Path: %s, data: %s", crop_data(table_name), crop_data(str(data)))
        return self._run(_data)

    @connected
    def insert(self, table_name: str, data: Dict) -> SurrealResult:
        _data = {"method": "insert", "params": [table_name, data]}
        logger.info("Operation: INSERT. Path: %s, data: %s", crop_data(table_name), crop_data(str(data)))
        return self._run(_data)

    @connected
    def update(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        _data = {"method": "update", "params": [table_name, data]}
        logger.info("Operation: UPDATE. Path: %s, data: %s", crop_data(table_name), crop_data(str(data)))
        return self._run(_data)

    @connected
    def merge(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        _data = {"method": "merge", "params": [table_name, data]}
        logger.info("Operation: MERGE. Path: %s, data: %s", crop_data(table_name), crop_data(str(data)))
        return self._run(_data)

    @connected
    def patch(self, table_name: str, data: Union[Dict, List], record_id: Optional[str] = None,
              return_diff: bool = False) -> SurrealResult:
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        params = [table_name, data]
        if return_diff:
            params.append(return_diff)
        _data = {"method": "patch", "params": params}
        logger.info("Operation: PATCH. Path: %s, data: %s, use DIFF: %s", crop_data(table_name), crop_data(str(data)),
                    return_diff)
        return self._run(_data)

    @connected
    def delete(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        _data = {"method": "delete", "params": [table_name]}
        logger.info("Operation: DELETE. Path: %s", crop_data(table_name))
        return self._run(_data)

    def close(self):
        super().close()
        if self.is_connected():
            self.client.close()

    def is_connected(self) -> bool:
        return self.client.connected

    def _run(self, data, callback: Callable = None):
        return self.client.send(data, callback)
