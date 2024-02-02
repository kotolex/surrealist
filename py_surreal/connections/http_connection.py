from logging import getLogger
from pathlib import Path
from typing import Tuple, Dict, Optional, Union

from py_surreal.clients.http_client import HttpClient
from py_surreal.connections.connection import Connection, connected
from py_surreal.errors import SurrealConnectionError, HttpClientError, CompatibilityError, HttpConnectionError
from py_surreal.utils import (ENCODING, to_result, SurrealResult, DEFAULT_TIMEOUT, crop_data, mask_pass, HTTP_OK)

logger = getLogger("http_connection")


class HttpConnection(Connection):
    """
    Represents http transport and abilities to work with SurrealDb. It is not recommended connection, use websocket on
    any doubt, this connection exists for compatibility reasons. Some features as live query is not possible on this
    transport, but import and export are.
    Refer to py_surreal documentation and here https://docs.surrealdb.com/docs/integration/http

    Each method call creates new http short-live connection.

    On creating, this object tries to create connection with specified data and will raise exception on fail.
    """

    def __init__(self, url: str, db_params: Optional[Dict] = None, credentials: Tuple[str, str] = None,
                 timeout: int = DEFAULT_TIMEOUT):
        super().__init__(db_params, credentials, timeout)
        self.http_client = HttpClient(url, headers=db_params, credentials=credentials, timeout=timeout)
        try:
            is_ready = self._is_ready()
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

    def _is_ready(self) -> bool:
        return self._simple_get("status")[0] == HTTP_OK and self._simple_get("health")[0] == HTTP_OK

    @connected
    def signin(self, user: str, password: str, namespace: Optional[str] = None, database: Optional[str] = None,
               scope: Optional[str] = None) -> SurrealResult:
        """
        This method allows you to sign in a root, namespace, database or scope user against SurrealDB

        Refer to: https://docs.surrealdb.com/docs/integration/http#signin

        Example:
        http_connection.signin('root', 'root') # sign in as root user
        http_connection.signin('root', 'root', namespace='test', database='test') # sign in as db user


        :param user: name of the user
        :param password: user password
        :param namespace: name of the namespace to use
        :param database: name of the database to use
        :param scope: name of the scope to use
        :return: result of request
        """
        opts = {"user": user, "pass": password}
        if namespace:
            opts['ns'] = namespace
        if database:
            opts['db'] = database
        if scope:
            opts['scope'] = scope
        logger.info("Operation: SIGNIN. Data: %s", crop_data(mask_pass(str(opts))))
        text = raise_if_not_http_ok(self._simple_request("POST", "signin", opts))
        return to_result(text)

    @connected
    def signup(self, namespace: str, database: str, scope: str, params: Optional[Dict] = None) -> SurrealResult:
        """
        This method allows you to sign up a user

        Refer to: https://docs.surrealdb.com/docs/integration/http#signup

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/scope

        Example:
        http_connection.signup(namespace='test', database='test', scope='user_scope',
                              params={'user': 'john:doe', 'pass': '123456'})

        :param namespace: name of the namespace to use
        :param database: name of the database to use
        :param scope: name of the scope to use
        :param params: dict with user and pass, for example {"user":"root", "pass":"root"}
        :return: result of request
        """
        params = params or {}
        body = {"ns": namespace, "db": database, "sc": scope, **params}
        logger.info("Operation: SIGNUP. Data: %s", crop_data(mask_pass(str(body))))
        text = raise_if_not_http_ok(self._simple_request("POST", "signup", body))
        return to_result(text)

    @connected
    def select(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method selects either all records in a table or a single record

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#get-table
        https://docs.surrealdb.com/docs/integration/http#get-record

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/select

        Examples:
        http_connection.select("article") # select all records in article table
        http_connection.select("article:first") # select one article with id 'first'
        http_connection.select("article", "first") # select one article with id 'first', analog of previous

        Notice: do not specify id twice: in table name and in record_id, it will cause error on SurrealDB side

        :param table_name: table name or table name with record_id to select
        :param record_id: optional parameter, if exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        logger.info("Operation: SELECT. Path: %s", crop_data(url))
        text = raise_if_not_http_ok(self._simple_get(url))
        return to_result(text)

    @connected
    def create(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method creates a record either with a random or specified record_id. If no id specified in record_id or
        in data arguments, then id will be generated by SurrealDB

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#post-table
        https://docs.surrealdb.com/docs/integration/http#post-record

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/create

        Examples:
        http_connection.create("person", {"name": "John Doe"}) # create one record in person table with random id
        http_connection.create("person", {"id":"my_id", "name": "John Doe"}) # create one record in person table
        with specified id
        http_connection.create("person", {"name": "John Doe"}, "my_id") # create one record in person table
        with specified id
        http_connection.create("person:my_id", {"name": "John Doe"}) # create one record in person table
        with specified id

        Notice: do not specify id twice, for example in table name and in data, it will cause error on SurrealDB side


        :param table_name: table name or table name with record_id to create
        :param data: dict with data to create
        :param record_id: optional parameter, if exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        logger.info("Operation: CREATE. Path: %s, data: %s", crop_data(url), crop_data(str(data)))
        text = raise_if_not_http_ok(self._simple_request("POST", url, data))
        return to_result(text)

    @connected
    def update(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method can be used to update or modify records in the database. So all old fields will be deleted and new
        will be added, if you wand just to add field to record, keeping old ones - use merge method instead

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#put-table
        https://docs.surrealdb.com/docs/integration/http#put-record

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/update

        Example:
        http_connection.update("person:my_id", {"name": "Alex Doe"}) # record with specified id will be now
        {"name": "Alex Doe"}, all data stored in record before will be deleted
        http_connection.update("person", {"name": "Alex Doe"}, "my_id") # record with specified id will be now
        {"name": "Alex Doe"}, all data stored in record before will be deleted

        Notice: do not specify id twice, for example in table name and in data, it will cause error on SurrealDB side

        :param table_name: table name or table name with record_id to update
        :param data: dict with data to create
        :param record_id: optional parameter, if exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        logger.info("Operation: UPDATE. Path: %s, data: %s", crop_data(url), crop_data(str(data)))
        text = raise_if_not_http_ok(self._simple_request("PUT", url, data))
        return to_result(text)

    @connected
    def delete(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method deletes all records in a table or a single record, be careful and do not forget to specify id if you
        do not want to delete all records. This method do not remove table itself, only records in it

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#delete-table
        https://docs.surrealdb.com/docs/integration/http#delete-record

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/delete

        Examples:
        http_connection.delete("person:my_id") # deletes one record in person table
        http_connection.delete("person", "my_id") # deletes one record in person table
        http_connection.delete("person") # deletes all records in person table

        :param table_name: table name or table name with record_id to delete
        :param record_id: optional parameter, if exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        logger.info("Operation: DELETE. Path: %s", crop_data(url))
        text = raise_if_not_http_ok(self._simple_request("DELETE", url, {}))
        return to_result(text)

    @connected
    def merge(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method merges specified data into either all records in a table or a single record. Old data in records
        will not be deleted, if you want to replace old data with new - use update method

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#patch-table
        https://docs.surrealdb.com/docs/integration/http#patch-record

        Examples:
        http_connection.merge("person",{"active": True}) # "active" will be added to all records in person table
        http_connection.merge("person:my_id", {"active": True}) # "active" will be added to one record in person
        table with specified id

        :param table_name: table name or table name with record_id to merge
        :param data: dict with data to add
        :param record_id: optional parameter, if exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        logger.info("Operation: PATCH. Path: %s, data: %s", crop_data(url), crop_data(str(data)))
        text = raise_if_not_http_ok(self._simple_request("PATCH", url, data))
        return to_result(text)

    @connected
    def query(self, query: str, variables: Optional[Dict] = None) -> SurrealResult:
        """
        This method used for execute a custom SurrealQL query

        Attention! Http transport ignores variables, embed them into your query or switch to websocket connection

        Refer to: https://docs.surrealdb.com/docs/integration/http#sql

        For SurrealQL refer to: https://docs.surrealdb.com/docs/surrealql/overview

        Example:
        http_connection.query("SELECT * FROM article;") # gets all records from article table

        :param query: any SurrealQL query to execute
        :param variables: a set of variables used by the query
        :return: result of request
        """
        if variables is not None:
            logger.warning("Variables parameter cant be used on QUERY with http-connection, "
                           "embed them into your query or switch to websocket connection")
        logger.info("Operation: QUERY. Query: %s", crop_data(query))
        text = raise_if_not_http_ok(self._simple_request("POST", "sql", query, not_json=True))
        return to_result(text)

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
            text = raise_if_not_http_ok(self._simple_request("POST", "import", file.read().decode(ENCODING),
                                                             not_json=True))
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
            text = raise_if_not_http_ok(self._simple_request("POST", "ml/import", file.read().decode(ENCODING),
                                                             not_json=True))
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
    def patch(self, table_name: str, data: Dict, record_id: Optional[str] = None, return_diff: bool = False):
        """
        Http transport can not use patch operation, so you can use websocket transport for that, or methods like
        **merge** and **update** for same results


        :raise CompatibilityError: on any use
        """
        message = "Http transport can not use PATCH operation, use websocket transport or methods like merge and update"
        logger.error(message)
        raise CompatibilityError(message)

    def live(self, table_name, callback, need_diff: bool = False):
        """
        Http transport can not use live queries, so you should use websocket transport for that

        :raise CompatibilityError: on any use
        """
        message = "Http transport can not use live queries, use websockets instead"
        logger.error(message)
        raise CompatibilityError(message)

    def kill(self, live_query_id: str):
        """
        Http transport can not use KILL operation, so you should use websocket transport for that

        :raise CompatibilityError: on any use
        """
        message = "Http transport can not use kill, use websockets instead"
        logger.error(message)
        raise CompatibilityError(message)

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
