import urllib.parse
from logging import getLogger
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union

from surrealist.clients.ws_client import WebSocketClient
from surrealist.connections.connection import Connection, connected
from surrealist.enums import Transport
from surrealist.errors import (CompatibilityError, SurrealConnectionError,
                               WebSocketConnectionClosedError,
                               WebSocketConnectionError)
from surrealist.result import SurrealResult
from surrealist.utils import AC, DB, DEFAULT_TIMEOUT, NS

logger = getLogger("surrealist.connections.websocket")


class WebSocketConnection(Connection):
    """
    Represents websocket transport and abilities to work with SurrealDb. It is a recommended connection.

    Refer to surrealist documentation: https://github.com/kotolex/surrealist?tab=readme-ov-file#transports

    Refer to: https://docs.surrealdb.com/docs/integration/websocket

    Each object creates only one websocket connection and can be used in the context manager to close properly.
    You cannot and should not try to use this object after closing connection. Just create a new connection.

    On creating, this object tries to create connection with specified parameters and will raise exception on fail.
    If namespace and database specified - USE method are called automatically
    If credentials specified - SIGNIN are called automatically
    """

    def __init__(self, url: str, db_params: Optional[Dict] = None, credentials: Optional[Tuple[str, str]] = None,
                 timeout: int = DEFAULT_TIMEOUT):
        super().__init__(db_params, credentials, timeout)
        self._url = url
        base_url = urllib.parse.urlparse(url.lower())
        self._db_params = {}
        if db_params:
            if NS in db_params:
                self._db_params[NS] = db_params[NS]
            if DB in db_params:
                self._db_params[DB] = db_params[DB]
            if AC in db_params:
                self._db_params[DB] = db_params[AC]
        if base_url.scheme in ("ws", "wss"):
            self._base_url = url
        if base_url.scheme in ("http", "https"):
            self._base_url = f"{base_url.scheme.replace('http', 'ws')}://{base_url.netloc}/rpc"
        try:
            self._client = WebSocketClient(self._base_url, timeout)
        except TimeoutError:
            logger.error("Cant connect to %s in %s seconds", self._base_url, self._timeout)
            raise SurrealConnectionError(f"Cant connect to {self._base_url} in {timeout} seconds.\n"
                                         f"Is your SurrealDB started and work on that url? "
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")
        except WebSocketConnectionClosedError:
            logger.error("Cant connect to %s, connection refused", self._base_url)
            raise SurrealConnectionError(f"Cant connect to {self._base_url}, connection refused.\n"
                                         f"Is your SurrealDB started and work on that url? "
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")
        self._use_or_sign_on_params(credentials)
        self._connected = True
        masked_creds = None if not credentials else (credentials[0], "******")
        logger.info("Connected to %s, params: %s, credentials: %s, timeout: %s", self._base_url, db_params,
                    masked_creds, timeout)

    def _use_rpc(self, data) -> SurrealResult:
        return self._run(data)

    def _use_or_sign_on_params(self, credentials):
        if self._db_params:
            ns = self._db_params.get(NS)
            db = self._db_params.get(DB)
            ac = self._db_params.get(AC)
            if credentials:
                self._user, self._pass = self._credentials
                signin_result = self._signin(self._user, self._pass, ns, db, ac)
                if signin_result.is_error():
                    logger.error("Error on connecting to %s. Info %s", self._base_url, signin_result)
                    raise WebSocketConnectionError(f"Error on connecting to {self._base_url}.\n"
                                                   f"Info: {signin_result.result}")
                self._token = signin_result.result
            else:
                use_result = self.use(ns, db)
                if use_result.is_error():
                    logger.error("Error on use %s. Info %s", self._db_params, use_result)
                    raise WebSocketConnectionError(f"Error on use '{self._db_params}'.\nInfo: {use_result.result}")
        else:
            if credentials:
                self._user, self._pass = self._credentials
                signin_result = self._signin(self._user, self._pass)
                if signin_result.is_error():
                    logger.error("Error on connecting to %s. Info %s", self._base_url, signin_result)
                    raise WebSocketConnectionError(f"Error on connecting to '{self._base_url}'.\nInfo: {signin_result}")
                self._token = signin_result.result

    def transport(self) -> Transport:
        """
        Returns the transport type for websocket connection
        """
        return Transport.WEBSOCKET

    @connected
    def use(self, namespace: str, database: Optional[str] = None) -> SurrealResult:
        """
        This method specifies the namespace and database for the current connection.
        For websocket connection, you should specify both.

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#use

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/use

        Example:
        websocket_connection.use("test", "test") # use test namespace and test database for this connection

        :param namespace: name of the namespace to use
        :param database: name of the database to use
        :return: result of request
        """
        if not database:
            # For some reason, websocket connection cannot work with only namespace
            msg = "Both namespace and database are required"
            logger.error(msg)
            raise CompatibilityError(msg)
        params = [namespace, database]
        data = {"method": "use", "params": params}
        logger.info("Operation: USE. Namespace: %s, database %s", namespace, database or "None")
        result = self._run(data)
        if not result.is_error():
            # if USE was OK we need to store new data (ns and db)
            self._db_params = {"NS": namespace, "DB": database}
        return result

    @connected
    def live(self, table_name: str, callback: Callable[[Dict], Any], return_diff: bool = False) -> SurrealResult:
        """
        This method can be used to initiate live query - a real-time selection from a table

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#live

        Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/live

        About DIFF refer to: https://jsonpatch.com

        Please see surrealist documentation: https://github.com/kotolex/surrealist?tab=readme-ov-file#live-query

        Example:
        websocket_connection.live("article", callback=lambda a_dict: print(a_dict)) # creates a live query to check any
        events on article table and just printing all incoming results

        :param table_name: name of the table to observe
        :param callback: a function to call on any incoming event. It should take one argument - a dict
        :param return_diff: True if you want to get only DIFF info on table events, False for a standard results
        :return: result of request with the live_id in 'result' field
        """
        params = [table_name]
        if return_diff:
            params.append(True)
        data = {"method": "live", "params": params}
        logger.info("Operation: LIVE. Data: %s", params)
        return self._run(data, callback)

    @connected
    def custom_live(self, custom_query: str, callback: Callable[[Dict], Any]) -> SurrealResult:
        """
        This method can be used to initiate custom live query - a real-time selection from a table with filters and
        other features of Live Query

        Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/live

        Please see surrealist documentation: https://github.com/kotolex/surrealist?tab=readme-ov-file#live-query

        Note: all results, DIFF, formats etc. should be specified in the query itself

        Example:
        ws.custom_live("LIVE SELECT * FROM person WHERE age > 18;", callback=lambda a_dict: print(a_dict)) # creates
        a live query to check any events on person table with records, where age field is bigger than 18, and just
        printing all incoming results

        :param custom_query: full LIVE SELECT query text
        :param callback: a function to call on any incoming event. It should take one argument - a dict
        :return: result of request with the live_id in 'result' field
        """
        data = {"method": "query", "params": [custom_query], "additional": "live"}
        logger.info("Operation: CUSTOM LIVE. Query: %s", custom_query)
        result = self._run(data, callback)
        result.query = custom_query
        return result

    @connected
    def kill(self, live_query_id: str) -> SurrealResult:
        """
        This method is used to terminate a running live query by id

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#kill

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/kill

        :param live_query_id: id for the query to kill
        :return: result of request
        """
        data = {"method": "kill", "params": [live_query_id]}
        logger.info("Operation: KILL. Live_id: %s", live_query_id)
        return self._run(data)

    def export(self):
        """
        Websocket transport cannot use export operation, so you can use http transport for that, or SurrealDB tools

        Refer to: https://docs.surrealdb.com/docs/cli/export

        :raise CompatibilityError: on any use
        """
        message = "Export is not allowed for websocket transport in the current SurrealDB version"
        logger.error(message)
        full_message = f"{message}\nYou can use http transport or abilities of SurrealDb itself\n" \
                       f"Refer to: https://docs.surrealdb.com/docs/cli/export"
        raise CompatibilityError(full_message)

    def ml_export(self, _name: str, _version: str):
        """
        Websocket transport cannot use ML export operation, so you can use http transport for that, or SurrealDB tools

        Refer to: https://docs.surrealdb.com/docs/cli/ml/export

        :raise CompatibilityError: on any use
        """
        message = "ML export is not allowed for websocket transport in the current SurrealDB version"
        logger.error(message)
        full_message = f"{message}\nYou can use http transport or abilities of SurrealDb itself\n" \
                       f"Refer to: https://docs.surrealdb.com/docs/cli/ml/export"
        raise CompatibilityError(full_message)

    def import_data(self, _path: Union[str, Path]):
        """
        Websocket transport cannot use import operation, so you can use http transport for that, or SurrealDB tools

        Refer to: https://docs.surrealdb.com/docs/cli/import

        :raise CompatibilityError: on any use
        """
        message = "Import is not allowed for websocket transport in the current SurrealDB version"
        logger.error(message)
        full_message = f"{message}\nYou can use http transport or abilities of SurrealDb itself\n" \
                       f"Refer to: https://docs.surrealdb.com/docs/cli/import"
        raise CompatibilityError(full_message)

    def ml_import(self, _path: Union[str, Path]):
        """
        Websocket transport cannot use ML import operation, so you can use http transport for that, or SurrealDB tools

        Refer to: https://docs.surrealdb.com/docs/cli/ml/import

        :raise CompatibilityError: on any use
        """
        message = "ML import is not allowed for websocket transport in the current SurrealDB version"
        logger.error(message)
        full_message = f"{message}\nYou can use http transport or abilities of SurrealDb itself\n" \
                       f"Refer to: https://docs.surrealdb.com/docs/cli/ml/import"
        raise CompatibilityError(full_message)

    def close(self):
        super().close()
        if self.is_connected():
            self._client.close()

    def is_connected(self) -> bool:
        return self._client.is_connected()

    def _run(self, data, callback: Callable = None) -> SurrealResult:
        result = self._client.send(data, callback)
        logger.info("Got result: %s", result)
        return result
