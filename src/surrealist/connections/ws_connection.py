import urllib.parse
from logging import getLogger
from pathlib import Path
from typing import Optional, Tuple, Dict, Union, List, Callable, Any

from surrealist.clients.ws_client import WebSocketClient
from surrealist.connections.connection import Connection, connected
from surrealist.errors import (SurrealConnectionError, WebSocketConnectionError, ConnectionParametersError,
                               WebSocketConnectionClosedError, CompatibilityError)
from surrealist.result import SurrealResult
from surrealist.utils import DEFAULT_TIMEOUT, crop_data, mask_pass, NS, DB

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
        base_url = urllib.parse.urlparse(url.lower())
        self._params = {}
        if db_params:
            if NS in db_params:
                self._params[NS] = db_params[NS]
            if DB in db_params:
                self._params[DB] = db_params[DB]
            if not self._params:
                message = "Connection parameters namespace and database required"
                logger.error(message)
                raise ConnectionParametersError(message)
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

    def _use_or_sign_on_params(self, credentials):
        if self._params:
            if credentials:
                self._user, self._pass = self._credentials
                signin_result = self.signin(self._user, self._pass, *list(self._params.values()))
                if signin_result.is_error():
                    logger.error("Error on connecting to %s. Info %s", self._base_url, signin_result)
                    raise WebSocketConnectionError(f"Error on connecting to {self._base_url}.\n"
                                                   f"Info: {signin_result.result}")
            else:
                use_result = self.use(self._params["NS"], self._params["DB"])
                if use_result.is_error():
                    logger.error("Error on use %s. Info %s", self._params, use_result)
                    raise WebSocketConnectionError(f"Error on use '{self._params}'.\nInfo: {use_result.result}")
        else:
            if credentials:
                self._user, self._pass = self._credentials
                signin_result = self.signin(self._user, self._pass)
                if signin_result.is_error():
                    logger.error("Error on connecting to %s. Info %s", self._base_url, signin_result)
                    raise WebSocketConnectionError(f"Error on connecting to '{self._base_url}'.\nInfo: {signin_result}")

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
        logger.info("Operation: USE. Namespace: %s, database %s", crop_data(namespace), crop_data(database or "None"))
        result = self._run(data)
        if not result.is_error():
            # if USE was OK we need to store new data (ns and db)
            self._params = {"NS": namespace, "DB": database}
        return result

    @connected
    def signin(self, user: str, password: str, namespace: Optional[str] = None, database: Optional[str] = None,
               scope: Optional[str] = None) -> SurrealResult:
        """
        This method allows you to sign in a root, namespace, database or scope user against SurrealDB

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#signin

        Example:
        websocket_connection.signin('root', 'root') # sign in as root user
        websocket_connection.signin('root', 'root', namespace='test', database='test') # sign in as db user

        :param user: name of the user
        :param password: password for auth
        :param namespace: name of the namespace to use
        :param database: name of the database to use
        :param scope: name of the scope to use
        :return: result of request
        """
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
        """
        This method allows you to sign up a user

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#signup

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/scope

        Example:
        websocket_connection.signup(namespace='test', database='test', scope='user_scope',
                              params={'user': 'john:doe', 'pass': '123456'})

        :param namespace: name of the namespace to use
        :param database: name of the database to use
        :param scope: name of the scope to use
        :param params: dict with user and pass, for example {"user":"root", "pass":"root"}
        :return: result of request
        """
        params = params or {}
        params = {"NS": namespace, "DB": database, "SC": scope, **params}
        data = {"method": "signup", "params": [params]}
        logger.info("Operation: SIGNUP. Data: %s", crop_data(mask_pass(str(params))))
        return self._run(data)

    @connected
    def authenticate(self, token: str) -> SurrealResult:
        """
        This method authenticates user with given token

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#authenticate

        :param token: token for auth
        :return: result of request
        """
        data = {"method": "authenticate", "params": [token]}
        logger.info("Operation: AUTHENTICATE. Token: %s", crop_data(token))
        return self._run(data)

    @connected
    def invalidate(self) -> SurrealResult:
        """
        This method will invalidate the user's session for the current connection

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#invalidate

        :return: result of request
        """
        data = {"method": "invalidate"}
        logger.info("Operation: INVALIDATE")
        return self._run(data)

    @connected
    def let(self, name: str, value: Any) -> SurrealResult:
        """
        This method sets and stores a value which can then be used in a subsequent query

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#let

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/let

        :param name: name for the variable (without $ sign!)
        :param value: value for the variable
        :return: result of request
        """
        data = {"method": "let", "params": [name, value]}
        logger.info("Operation: LET. Name: %s, Value: %s", crop_data(name), crop_data(str(value)))
        return self._run(data)

    @connected
    def unset(self, name: str) -> SurrealResult:
        """
        This method unsets value, which was previously stored

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#unset

        :param name: name for the variable (without $ sign!)
        :return: result of request
        """
        data = {"method": "unset", "params": [name]}
        logger.info("Operation: UNSET. Variable name: %s", crop_data(name))
        return self._run(data)

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
        logger.info("Operation: LIVE. Data: %s", crop_data(str(params)))
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
        logger.info("Operation: CUSTOM LIVE. Query: %s", crop_data(custom_query))
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
        logger.info("Operation: KILL. Live_id: %s", crop_data(live_query_id))
        return self._run(data)

    @connected
    def query(self, query: str, variables: Optional[Dict] = None) -> SurrealResult:
        """
        This method used for execute a custom SurrealQL query

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#query

        For SurrealQL refer to: https://docs.surrealdb.com/docs/surrealql/overview

        Example:
        websocket_connection.query("SELECT * FROM article;") # gets all records from article table
        websocket_connection.query("SELECT * FROM type::table($tb);", {"tb": "article"}) # gets all records from
        article table using variable tb to specify table

        :param query: any SurrealQL query to execute
        :param variables: a set of variables used by the query
        :return: result of request
        """
        params = [query]
        if variables is not None:
            params.append(variables)
        data = {"method": "query", "params": params}
        logger.info("Operation: QUERY. Query: %s, variables: %s", crop_data(query), crop_data(str(variables)))
        result = self._run(data)
        result.query = params[0] if len(params) == 1 else params
        return result

    @connected
    def select(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method selects either all records in a table or a single record

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#select

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/select

        Examples:
        websocket_connection.select("article") # select all records in article table
        websocket_connection.select("article:first") # select one article with id 'first'
        websocket_connection.select("article", "first") # select one article with id 'first', analog of previous

        Notice: do not specify id twice: in table name and in record_id, it will cause error on SurrealDB side

        :param table_name: table name or table name with record_id to select
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        data = {"method": "select", "params": [table_name]}
        logger.info("Operation: SELECT. Path: %s", crop_data(table_name))
        result = self._run(data)
        if not isinstance(result.result, List) and not result.is_error():
            result.result = [result.result] if result.result else []
        return result

    @connected
    def create(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method creates a record either with a random or specified record_id. If no id specified in record_id or
        in data arguments, then id will be generated by SurrealDB

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#create

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/create

        Examples:
        websocket_connection.create("person", {"name": "John Doe"}) # create one record in person table with random id
        websocket_connection.create("person", {"id":"my_id", "name": "John Doe"}) # create one record in person table
        with specified id
        websocket_connection.create("person", {"name": "John Doe"}, "my_id") # create one record in person table
        with specified id
        websocket_connection.create("person:my_id", {"name": "John Doe"}) # create one record in person table
        with specified id

        Notice: do not specify id twice, for example, in table name and in data, it will cause error on SurrealDB side

        :param table_name: table name or table name with record_id to create
        :param data: dict with data to create
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        if record_id is not None:
            data["id"] = record_id
        _data = {"method": "create", "params": [table_name, data]}
        logger.info("Operation: CREATE. Path: %s, data: %s", crop_data(table_name), crop_data(str(data)))
        result = self._run(_data)
        if isinstance(result.result, List) and len(result.result) == 1:
            result.result = result.result[0]
        return result

    @connected
    def insert(self, table_name: str, data: Union[List, Dict]) -> SurrealResult:
        """
        This method inserts one or more records. If you specify recordID in data and record with that id already
        exists - no inserts or updates will happen and the content of the existing record will be returned. If you need
        to change existing record, please consider **update** or **merge**

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#insert

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/insert

        Examples:
        websocket_connection.insert("person", {"name": "John Doe"}) # inserts one record with random id
        websocket_connection.insert("person", [{"name": "John Doe"}, {"name", "Jane Doe"}]) # inserts two records
        with random ids

        Note: do not use record id in table_name parameter (table:recordID) - it will cause error on SurrealDB side

        :param table_name: table name or table name with record_id to insert
        :param data: dict or list(many records) with data to create
        :return: result of request
        """
        _data = {"method": "insert", "params": [table_name, data]}
        logger.info("Operation: INSERT. Path: %s, data: %s", crop_data(table_name), crop_data(str(data)))
        return self._run(_data)

    @connected
    def update(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method can be used to update or modify records in the database. So all old fields will be deleted, and new
        will be added, if you wand just to add field to record, keeping old ones -use **merge** method instead.If
        record with specified id does not exist it will be created, if it exists - all fields will be replaced

        Note: if you want to create/replace one record, you should specify recordID in table_name or in record_id, but
        not in data parameters.

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#update

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/update

        Example:
        websocket_connection.update("person:my_id", {"name": "Alex Doe"}) # record with specified id will be now
        {"name": "Alex Doe"}, all data stored in record before will be deleted
        websocket_connection.update("person", {"name": "Alex Doe"}, "my_id") # record with specified id will be now
        {"name": "Alex Doe"}, all data stored in record before will be deleted

        Notice: do not specify id twice, for example, in table name and in record_id, it will cause error

        :param table_name: table name or table name with record_id to update
        :param data: dict with data to create
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        _data = {"method": "update", "params": [table_name, data]}
        logger.info("Operation: UPDATE. Path: %s, data: %s", crop_data(table_name), crop_data(str(data)))
        return self._run(_data)

    @connected
    def merge(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method merges specified data into either all records in a table or a single record. Old data in records
        will not be deleted, if you want to replace old data with new - use **update** method.If
        record with specified id does not exist, it will be created.

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#merge

        Examples:
        websocket_connection.merge("person",{"active": True}) # "active" will be added to all records in person table
        websocket_connection.merge("person:my_id", {"active": True}) # "active" will be added to one record in person
        table with specified id

        :param table_name: table name or table name with record_id to merge
        :param data: dict with data to add
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        _data = {"method": "merge", "params": [table_name, data]}
        logger.info("Operation: MERGE. Path: %s, data: %s", crop_data(table_name), crop_data(str(data)))
        return self._run(_data)

    @connected
    def patch(self, table_name: str, data: Union[Dict, List], record_id: Optional[str] = None,
              return_diff: bool = False) -> SurrealResult:
        """
        This method changes specified data in one ar all records. If given table does not exist, new table and record
        will not be created if table exists but no such record_id - new record will be created, if no record id-all
        records will be transformed

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#patch

        About allowed data format and DIFF refer to: https://jsonpatch.com

        Examples:
        websocket_connection.patch("person", [{"op": "replace", "path": "/active", "value": False}]) # replaces active
        field for all records in person table to False
        websocket_connection.patch("person:my_id", [{"op": "replace", "path": "/active", "value": False}]) # replaces
        active field for one record with specified id to False

        Notice: do not specify id twice, for example, in table name and in data, it will cause error on SurrealDB side

        :param table_name: table name or table name with record_id to patch
        :param data: list with json-patch data
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :param return_diff: True if you want to get only DIFF info, False for a standard results
        :return: result of request
        """
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
        """
        This method deletes all records in a table or a single record, be careful and don't forget to specify id if you
        do not want to delete all records. This method does not remove table itself, only records in it.As a result of
        this method you will get all deleted records or None if no such record or table

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#delete

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/delete

        Examples:
        websocket_connection.delete("person:my_id") # deletes one record in person table
        websocket_connection.delete("person", "my_id") # deletes one record in person table
        websocket_connection.delete("person") # deletes all records in person table

        :param table_name: table name or table name with record_id to delete
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        table_name = table_name if record_id is None else f"{table_name}:{record_id}"
        _data = {"method": "delete", "params": [table_name]}
        logger.info("Operation: DELETE. Path: %s", crop_data(table_name))
        return self._run(_data)

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
        logger.info("Got result: %s", crop_data(str(result)))
        return result
