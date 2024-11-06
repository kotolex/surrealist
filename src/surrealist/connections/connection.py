from abc import ABC, abstractmethod
from functools import wraps
from logging import getLogger
from typing import Tuple, Dict, Optional, Union, List, Callable, Any

from surrealist.enums import Transport
from surrealist.errors import OperationOnClosedConnectionError, WrongParameterError
from surrealist.record_id import RecordId
from surrealist.result import SurrealResult
from surrealist.utils import (DEFAULT_TIMEOUT, NS, DB, AC, mask_pass, clean_dates, StrOrRecord,
                              get_table_or_record_id)

logger = getLogger("surrealist.connection")
LINK = "https://github.com/kotolex/surrealist?tab=readme-ov-file#recursion-and-json-in-python"


def connected(func):
    """
    Decorator for methods to make sure the underlying connection is alive (connected to DB)

    :param func: method to decorate
    :raise OperationOnClosedConnectionError: if connection is already closed
    """

    @wraps(func)
    def wrapped(*args, **kwargs):
        # args[0] is a self-argument in methods
        if not args[0].is_connected():
            message = "Your connection is already closed"
            logger.error(message, exc_info=False)
            raise OperationOnClosedConnectionError(message)
        return func(*args, **kwargs)

    return wrapped


class Connection(ABC):
    """
    Parent for connection objects, contains all public methods to work with API
    """

    def __init__(self, db_params: Optional[Dict] = None, credentials: Tuple[str, str] = None,
                 timeout: int = DEFAULT_TIMEOUT):
        """
        Init any connection to use
        :param db_params: optional parameter, if it is not None, should be like {"NS": "test", "DB": "test"}
        :param credentials: optional pair of user and pass for auth, like ("root", "root")
        :param timeout: timeout in seconds to wait connection results and responses
        """
        self._db_params = db_params
        self._credentials = credentials
        self._connected = False
        self._timeout = timeout
        self._token = None

    def close(self):
        """
        Closes the connection. You can not and should not use a connection object after that
        """
        logger.info("The connection was closed")
        self._connected = False

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()

    def is_connected(self) -> bool:
        """
        Checks the connection is still alive and usable

        :return: True if connection is usable, False otherwise
        """
        return self._connected

    @connected
    def count(self, table_name: str) -> SurrealResult:
        """
        Returns records count for given table. You should have permissions for this action.
        Actually converts to QL "SELECT count() FROM {table_name} GROUP ALL;" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/functions/count

        Note: returns zero if table does not exist, if you need to check table existence use **is_table_exists**

        Note: if you specify table_name with recordID like "person:john" you will get count of fields in record

        :param table_name: name of the table
        :return: result containing count, like SurrealResult(id='', error=None, result=[{'count': 1}], time='123.333µs')
        """
        logger.info("Query-Operation: COUNT. Table: %s", table_name)
        result = self.query(f"SELECT count() FROM {table_name} GROUP ALL;")
        if not result.is_error():
            result.result = self._get_count(result.result)
        return result

    @connected
    def table_info(self, table_name: str, structured: bool = False) -> SurrealResult:
        """
        Returns info about specified table. You should have permissions for this action.

        Actually converts to QL "INFO FOR TABLE table_name" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/info

        :param table_name: name of the table
        :param structured: if True returns data in structured view (use STRUCTURE statement). Note: experimental!
        :return: full table information
        """
        return self._info(f"TABLE {table_name}", structured)

    @connected
    def db_info(self, structured: bool = False) -> SurrealResult:
        """
        Returns info about a current database. You should have permissions for this action.

        Actually converts to QL "INFO FOR DB" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/info

        :param structured: if True, return data in structured view (use STRUCTURE statement). Note: experimental!
        :return: full database information
        """
        return self._info("DB", structured)

    @connected
    def ns_info(self, structured: bool = False) -> SurrealResult:
        """
        Returns info about current namespace. You should have permissions for this action.

        Actually converts to QL "INFO FOR NS" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/info

        :param structured: if True, return data in structured view (use STRUCTURE statement). Note: experimental!
        :return: full namespace information
        """
        return self._info("NS", structured)

    @connected
    def root_info(self, structured: bool = False) -> SurrealResult:
        """
        Returns info about root. You should have permissions for this action.

        Actually converts to QL "INFO FOR ROOT" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/info

        :param structured: if True, return data in structured view (use STRUCTURE statement). Note: experimental!
        :return: information about root
        """
        return self._info("ROOT", structured)

    def _info(self, type_: str, structured: bool = False) -> SurrealResult:
        if structured:
            type_ = f"{type_} STRUCTURE"
        logger.info("Query-Operation: %s_INFO", type_)
        return self.query(f"INFO FOR {type_};")

    @connected
    def session_info(self) -> SurrealResult:
        """
        Returns info about the current session. You should have permissions for this action.

        Actually converts to QL query to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/functions/session

        :return: full session information
        """
        query = 'RETURN {"db" : session::db(), "session_id" : session::id(), "ip" : session::ip(), ' \
                '"ns" : session::ns(), "http_origin" : session::origin(), "access" : session::ac()};'
        logger.info("Query-Operation: SESSION_INFO")
        return self.query(query)

    @connected
    def info(self) -> SurrealResult:
        """
        This method returns the record of an authenticated record user.
        The result property of the response is likely different depending on your schema and the authenticated user

        :return: result of the query
        """
        logger.info("Query-Operation: INFO")
        data = {"method": "info"}
        result = self._use_rpc(data)
        return result

    @connected
    def db_tables(self) -> SurrealResult:
        """
        Returns all tables names in the current database. You should have permissions for this action.

        Actually call **db_info** and parse tables attribute there.

        :return: list of all tables names
        """
        logger.info("Query-Operation: DB_TABLES")
        res: SurrealResult = self.db_info()
        res.result = list(res.result["tables"].keys())
        return res

    @connected
    def is_table_exists(self, table_name: str) -> bool:
        """
        Returns True if table with given name exists in a current database. You should have permissions for this action.

        :param table_name: name of the table, we do not expect record_id here
        :return: True if table exists, False otherwise
        """
        return table_name in self.db_tables().result

    @connected
    def remove_table(self, table_name: str, if_exists: bool = True) -> SurrealResult:
        """
        Fully removes table, even if it contains some records, analog of SQL "DROP table". You should have permissions
        for this action.This method cannot remove any other resource if you need to remove db, ns or access -
        use **query**

        If if_exists parameter is False and the table does not exist - error will be returned at a result.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/remove

        Note: only name of the table allowed here, do not use record_id

        :param table_name: the name of the table,
        :param if_exists: if True, will use IF EXISTS statement for the query
        :return: result of the query
        """
        logger.info("Query-Operation: REMOVE. Table name %s", table_name)
        add = "" if not if_exists else "IF EXISTS"
        return self.query(f"REMOVE TABLE {add} {table_name};")

    @connected
    def show_changes(self, table_name: str, since: str, limit: int = 10) -> SurrealResult:
        """
        This method can use Changes Feed feature of SurrealDB. You need a predefined CHANGEFEED table and database for
        using this action.

        Refer to: https://surrealdb.com/blog/unlocking-streaming-data-magic-with-surrealdb-live-queries-and-change-feeds

        Refer to: https://github.com/kotolex/surrealist?tab=readme-ov-file#change-feeds

        :param table_name: name of the table, no record_id expected here
        :param since: str representation of ISO date-time, for example d"2024-02-06T10:48:08.700483Z"
        :param limit: amount of changes to get
        :return: result of the query
        """
        query = f'SHOW CHANGES FOR TABLE {table_name} SINCE {since} LIMIT {limit};'
        return self.query(query)

    @abstractmethod
    def _use_rpc(self, data) -> SurrealResult:
        """
        Actual use of RPC protocol for a current connection type
        """

    def _signin(self, user: str, password: str, namespace: Optional[str] = None, database: Optional[str] = None,
                access: Optional[str] = None) -> SurrealResult:
        """
        This method allows you to sign in a root, namespace, database or scope user against SurrealDB

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#signin

        Example:
        connection.signin('root', 'root') # sign in as root user
        connection.signin('user_db', 'user_db', namespace='test', database='test') # sign in as db user

        :param user: name of the user
        :param password: password for auth
        :param namespace: name of the namespace to use
        :param database: name of the database to use
        :param access: name of the access method to use
        :return: result of request
        """
        params = {"user": user, "pass": password}
        if user is None or password is None:
            params = {}
        if namespace is not None:
            params[NS] = namespace
        if database is not None:
            params[DB] = database
        if access is not None:
            params[AC] = access
        data = {"method": "signin", "params": [params]}
        logger.info("Operation: SIGNIN. Data: %s", mask_pass(str(params)))
        return self._use_rpc(data)

    @abstractmethod
    def use(self, namespace: str, database: Optional[str] = None) -> SurrealResult:
        """
        This method specifies the namespace and optionally database for the current connection

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/use
        """

    @abstractmethod
    def transport(self) -> Transport:
        """
        This method returns the transport type for the current connection

        Refer to: https://github.com/kotolex/surrealist?tab=readme-ov-file#transports
        """

    @connected
    def let(self, name: str, value: Any) -> SurrealResult:
        """
        This method sets and stores a value which can then be used in a subsequent query.
        Http-transport cannot use the let method

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#let

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/let

        :param name: name for the variable (without $ sign!)
        :param value: value for the variable
        :return: result of request
        """
        data = {"method": "let", "params": [name, value]}
        logger.info("Operation: LET. Name: %s, Value: %s", name, value)
        return self._use_rpc(data)

    @connected
    def unset(self, name: str) -> SurrealResult:
        """
        This method unsets value, which was previously stored.
        Http-transport cannot use the unset method

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#unset

        :param name: name for the variable (without $ sign!)
        :return: result of request
        """
        data = {"method": "unset", "params": [name]}
        logger.info("Operation: UNSET. Variable name: %s", name)
        return self._use_rpc(data)

    @abstractmethod
    def live(self, table_name: str, callback: Callable[[Dict], Any], return_diff: bool = False) -> SurrealResult:
        """
        This method can be used to initiate live query - a real-time selection from a table. Works only for websockets.

        Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/live

        About DIFF refer to: https://jsonpatch.com

        Please see surrealist documentation: https://github.com/kotolex/surrealist?tab=readme-ov-file#live-query
        """

    @abstractmethod
    def custom_live(self, custom_query: str, callback: Callable[[Dict], Any]) -> SurrealResult:
        """
        This method can be used to initiate custom live query - a real-time selection from a table with filters and
        other features of Live Query. Works only for websockets.

        Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/live

        Please see surrealist documentation: https://github.com/kotolex/surrealist?tab=readme-ov-file#live-query

        Note: all results, DIFF, formats etc. should be specified in the query itself
        """

    @abstractmethod
    def kill(self, live_query_id: str) -> SurrealResult:
        """
        This method is used to terminate a running live query by id

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/kill
        """

    @connected
    def graphql(self, query: Dict, pretty: Optional[bool] = False) -> SurrealResult:
        """
        This method allows you to execute GraphQL queries against the database.
        The query parameter is a dictionary with the following fields:
        - query (required): The GraphQL query string.
        - variables or vars (optional): An object containing variables for the query.
        - operationName or operation (optional): The name of the operation to execute.

        Refer to: https://surrealdb.com/docs/surrealdb/integration/rpc#graphql

        Refer to: https://surrealdb.com/docs/surrealdb/querying/graphql

        Important Note: GraphQL validates all schemas for all tables in the database, so if there are some errors,
        you get an error back, even if the problem is not with your data

        Examples:
        connection.graphql({"query": "{ author { id name } }"}, pretty=True)

        :param query: dictionary with all parameters
        :param pretty: optional boolean parameter, indicating whether the output should be pretty-printed.
        :return: result of request
        :raise WrongParameterError: if query is not valid dictionary
        """
        allowed_fields = ("query", "variables", "vars", "operationName", "operation")
        if "query" not in query or any(field not in allowed_fields for field in query.keys()):
            raise WrongParameterError("Query parameter should be a dictionary with 3 fields\n"
                                      "Please see https://surrealdb.com/docs/surrealdb/integration/rpc#graphql")
        data = {"method": "graphql", "params": [query, {"pretty": pretty}]}
        logger.info("Operation: GRAPHQL. Query: %s, pretty: %s", query, pretty)
        result = self._use_rpc(data)
        return result

    @connected
    def run(self, func_name: str, version: Optional[str] = None, args: Optional[List] = None) -> SurrealResult:
        """
        This method allows you to execute built-in functions, custom functions, or machine learning models with
        optional arguments

        Refer to: https://surrealdb.com/docs/surrealdb/integration/rpc#run

        Examples:
        connection.run("time::now")

        :param func_name: The name of the function or model to execute. Prefix with fn:: for custom functions or
        ml:: for machine learning models.
        :param version: optional parameter, the version of the function or model to execute. When using a machine
        learning model (prefixed with ml::), the version parameter is required.
        :param args: The list of arguments to pass to the function or model.
        :return: result of request
        """
        data = {"method": "run", "params": [func_name]}
        if version is not None:
            data["params"].append(version)
        if args is not None:
            if len(data["params"]) == 1:
                data["params"].append(None)
            data["params"].append(args)
        logger.info("Operation: RUN. Function: %s, version: %s, args: %s", func_name, version, args)
        result = self._use_rpc(data)
        return result

    @connected
    def version(self) -> SurrealResult:
        """
        This method returns version information about the database/server

        Refer to: https://surrealdb.com/docs/surrealdb/integration/rpc#version

        Examples:
        connection.version() # get version information

        :return: result of request
        """
        data = {"method": "version"}
        logger.info("Operation: VERSION")
        result = self._use_rpc(data)
        return result

    @connected
    def select(self, table_name: str, record_id: Optional[StrOrRecord] = None) -> SurrealResult:
        """
        This method selects either all records in a table or a single record

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#select

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/select

        Examples:
        connection.select("article") # select all records in article table
        connection.select("article:first") # select one article with id 'first'
        connection.select("article", "first") # select one article with id 'first', analog of previous
        connection.select("article", RecordId("first", "article")) # analog of previous

        Notice: do not specify id twice: in table name and in record_id, it will cause error on SurrealDB side

        :param table_name: table name or table name with record_id to select
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        table_name = get_table_or_record_id(table_name, record_id)
        data = {"method": "select", "params": [table_name]}
        logger.info("Operation: SELECT. Table: %s", table_name)
        result = self._use_rpc(data)
        if not isinstance(result.result, List) and not result.is_error():
            result.result = [result.result] if result.result else []
        return result

    @connected
    def create(self, table_name: str, data: Dict, record_id: Optional[StrOrRecord] = None) -> SurrealResult:
        """
        This method creates a record either with a random or specified record_id. If no id specified in record_id or
        in data arguments, then id will be generated by SurrealDB

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#create

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/create

        Examples:
        connection.create("person", {"name": "John Doe"}) # create one record in person table with random id
        connection.create("person", {"id":"my_id", "name": "John Doe"}) # create one record in person table
        with specified id
        connection.create("person", {"name": "John Doe"}, "my_id") # create one record in person table with specified id
        connection.create("person:my_id", {"name": "John Doe"}) # create one record in person table with specified id

        Notice: do not specify id twice, for example, in table name and in data, it will cause error on SurrealDB side

        :param table_name: table name or table name with record_id to create
        :param data: dict with data to create
        :param record_id: optional parameter, it can be string or record_id object
        :return: result of request
        """
        if record_id is not None:
            if isinstance(record_id, str):
                record_id = RecordId(record_id, table=table_name)
            data["id"] = record_id.to_valid_string()
        _data = {"method": "create", "params": [table_name, data]}
        logger.info("Operation: CREATE. Table: %s, data: %s", table_name, data)
        result = self._use_rpc(_data)
        if isinstance(result.result, List) and len(result.result) == 1:
            result.result = result.result[0]
        return result

    @connected
    def update(self, table_name: str, data: Dict, record_id: Optional[StrOrRecord] = None) -> SurrealResult:
        """
        This method can be used to update or modify records in the database. So all old fields will be deleted, and new
        will be added, if you wand just to add field to record, keeping old ones -use **merge** method instead.
        If a record with specified id does not exist, it will NOT be created, use **upsert** for that.

        Note: if you want to create/replace one record, you should specify recordID in table_name or in record_id, but
        not in data parameters.

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#update

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/update

        Example:
        connection.update("person:my_id", {"name": "Alex Doe"}) # record with specified id will be now
        {"name": "Alex Doe"}, all data stored in record before will be deleted
        connection.update("person", {"name": "Alex Doe"}, "my_id") # record with specified id will be now
        {"name": "Alex Doe"}, all data stored in record before will be deleted

        Notice: do not specify id twice, for example, in table name and in record_id, it will cause error

        :param table_name: table name or table name with record_id to update
        :param data: dict with data to create
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        table_name = get_table_or_record_id(table_name, record_id)
        _data = {"method": "update", "params": [table_name, data]}
        logger.info("Operation: UPDATE. Table: %s, data: %s", table_name, data)
        return self._use_rpc(_data)

    @connected
    def upsert(self, table_name: str, data: Dict, record_id: Optional[StrOrRecord] = None) -> SurrealResult:
        """
        This method can be used to create or update records in the database. So all old fields will be deleted, and new
        will be added, if you wand just to add field to record, keeping old ones -use **merge** method instead.
        If a record with specified id does not exist, it will be created, if it exists - all fields will be replaced

        Note: if you want to create/replace one record, you should specify recordID in table_name or in record_id, but
        not in data parameters.

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#upsert

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/upsert

        Example:
        connection.upsert("person:my_id", {"name": "Alex Doe"}) # record with specified id will be now
        {"name": "Alex Doe"}, all data stored in record before will be deleted

        Notice: do not specify id twice, for example, in table name and in record_id, it will cause error

        :param table_name: table name or table name with record_id to upsert
        :param data: dict with data to create
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        table_name = get_table_or_record_id(table_name, record_id)
        _data = {"method": "upsert", "params": [table_name, data]}
        logger.info("Operation: UPSERT. Table: %s, data: %s", table_name, data)
        return self._use_rpc(_data)

    @connected
    def insert(self, table_name: str, data: Union[List, Dict]) -> SurrealResult:
        """
        This method inserts one or more records. If you specify recordID in data and record with that id already
        exists - no inserts or updates will happen and the content of the existing record will be returned. If you need
        to change existing record, please consider **update** or **merge**

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#insert

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/insert

        Examples:
        connection.insert("person", {"name": "John Doe"}) # inserts one record with random id
        connection.insert("person", [{"name": "John Doe"}, {"name", "Jane Doe"}]) # inserts two records with random ids

        Note: do not use record id in table_name parameter (table:recordID) - it will cause error on SurrealDB side

        :param table_name: table name or table name with record_id to insert
        :param data: dict or list(many records) with data to create
        :return: result of request
        """
        _data = {"method": "insert", "params": [table_name, data]}
        logger.info("Operation: INSERT. Table: %s, data: %s", table_name, data)
        return self._use_rpc(_data)

    @connected
    def insert_relation(self, table_name: Optional[str], data: [Dict]) -> SurrealResult:
        """
        This method inserts a new relation record into the database. You can specify the relation table to insert into
        and provide the data for the new relation.

        Refer to: https://surrealdb.com/docs/surrealdb/integration/rpc#insert_relation

        Examples:
        connection.insert_relation("likes",{
            "in": "user:alice",
            "out": "post:123",
            "since": "2024-09-15T12:34:56Z"
        })

        :param table_name: The name of the relation table to insert into. If None, the table is determined from the id
        field in the data.
        :param data: dict containing the data for the new relation record, including in, out, and any additional fields
        :return: result of request
        """
        data = {"method": "insert_relation", "params": [table_name, data]}
        logger.info("Operation: INSERT-RELATION. Table: %s, data: %s", table_name, data)
        result = self._use_rpc(data)
        return result

    @connected
    def merge(self, table_name: str, data: Dict, record_id: Optional[StrOrRecord] = None) -> SurrealResult:
        """
        This method merges specified data into either all records in a table or a single record. Old data in records
        will not be deleted, if you want to replace old data with new - use **update** method.If
        record with specified id does not exist, it will be created.

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#merge

        Examples:
        connection.merge("person",{"active": True}) # "active" will be added to all records in person table
        connection.merge("person:my_id", {"active": True}) # "active" will be added to one record in person
        table with specified id

        :param table_name: table name or table name with record_id to merge
        :param data: dict with data to add
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        table_name = get_table_or_record_id(table_name, record_id)
        _data = {"method": "merge", "params": [table_name, data]}
        logger.info("Operation: MERGE. Table: %s, data: %s", table_name, data)
        return self._use_rpc(_data)

    @connected
    def delete(self, table_name: str, record_id: Optional[StrOrRecord] = None) -> SurrealResult:
        """
        This method deletes all records in a table or a single record, be careful and don't forget to specify id if you
        do not want to delete all records. This method does not remove table itself, only records in it.As a result of
        this method you will get all deleted records or None if no such record or table

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#delete

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/delete

        Examples:
        connection.delete("person:my_id") # deletes one record in person table
        connection.delete("person", "my_id") # deletes one record in person table
        connection.delete("person") # deletes all records in person table

        :param table_name: table name or table name with record_id to delete
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :return: result of request
        """
        table_name = get_table_or_record_id(table_name, record_id)
        _data = {"method": "delete", "params": [table_name]}
        logger.info("Operation: DELETE. Table: %s", table_name)
        return self._use_rpc(_data)

    @connected
    def patch(self, table_name: str, data: Union[Dict, List], record_id: Optional[StrOrRecord] = None,
              return_diff: bool = False) -> SurrealResult:
        """
        This method changes specified data in one ar all records. If given table does not exist, new table and record
        will not be created if table exists but no such record_id - new record will be created, if no record id-all
        records will be transformed

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#patch

        About allowed data format and DIFF refer to: https://jsonpatch.com

        Examples:
        connection.patch("person", [{"op": "replace", "path": "/active", "value": False}]) # replaces active
        field for all records in person table to False
        connection.patch("person:my_id", [{"op": "replace", "path": "/active", "value": False}]) # replaces
        active field for one record with specified id to False

        Notice: do not specify id twice, for example, in table name and in data, it will cause error on SurrealDB side

        :param table_name: table name or table name with record_id to patch
        :param data: list with json-patch data
        :param record_id: optional parameter, if it exists it will transform table_name to "table_name:record_id"
        :param return_diff: True if you want to get only DIFF info, False for a standard results
        :return: result of request
        """
        table_name = get_table_or_record_id(table_name, record_id)
        params = [table_name, data]
        if return_diff:
            params.append(return_diff)
        _data = {"method": "patch", "params": params}
        logger.info("Operation: PATCH. Table: %s, data: %s, use DIFF: %s", table_name, data, return_diff)
        return self._use_rpc(_data)

    @connected
    def query(self, query: str, variables: Optional[Dict] = None) -> SurrealResult:
        """
        This method used for execute a custom SurrealQL query

        Refer to: https://docs.surrealdb.com/docs/integration/websocket#query

        For SurrealQL refer to: https://docs.surrealdb.com/docs/surrealql/overview

        Example:
        connection.query("SELECT * FROM article;") # gets all records from article table
        connection.query("SELECT * FROM type::table($tb);", {"tb": "article"}) # gets all records from
        article table using variable tb to specify table

        :param query: any SurrealQL query to execute
        :param variables: a set of variables used by the query
        :return: result of request
        """
        query = clean_dates(query)
        params = [query]
        if variables is not None:
            params.append(variables)
        data = {"method": "query", "params": params}
        logger.info("Operation: QUERY. Query: %s, variables: %s", query, variables)
        result = self._use_rpc(data)
        result.query = params[0] if len(params) == 1 else params
        return result

    @connected
    def relate(self, relate_to: str, relation_table: str, relate_from: str,
               data: Optional[Dict] = None) -> SurrealResult:
        """
        This method relates two records with a specified relation

        Refer to: https://surrealdb.com/docs/surrealdb/integration/rpc#relate

        Examples:
        connection.relate("person:john", "knows", "person:jane")

        :param relate_to: The record to relate to
        :param relation_table: name of the relation table
        :param relate_from: The record to relate from
        :param data: dict containing the data for the new record
        :return: result of request
        """
        full_data = {"method": "relate", "params": [relate_to, relation_table, relate_from]}
        if data is not None:
            full_data["params"].append(data)
        logger.info("Operation: RELATE. Relate_to: %s, relation_table: %s, relate_from: %s, data: %s", relate_to,
                    relation_table, relate_from, data)
        result = self._use_rpc(full_data)
        return result

    @abstractmethod
    def import_data(self, path) -> SurrealResult:
        """
        This method imports a SurrealQL script file into a local or remote SurrealDB database server.

        Note: websocket connection cannot use this method

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#import
        https://docs.surrealdb.com/docs/cli/import
        """

    @abstractmethod
    def export(self) -> str:
        """
        This method exports all data for a specific namespace and database.

        Note: websocket connection cannot use this method

        Refer to: https://docs.surrealdb.com/docs/integration/http#export

        Refer to: https://docs.surrealdb.com/docs/cli/export/
        """

    @abstractmethod
    def ml_import(self, path) -> SurrealResult:
        """
        This method imports a SurrealQL ML file into a local or remote SurrealDB database server.

        Note: websocket connection cannot use this method

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#ml-import
        https://docs.surrealdb.com/docs/cli/ml/import
        """

    @abstractmethod
    def ml_export(self, name: str, version: str) -> str:
        """
        This method exports a SurrealML machine learning model from a specific namespace and database.
        As machine learning files can be large, the endpoint outputs a chunked HTTP response.

        Note: websocket connection cannot use this method

        Refer to: https://docs.surrealdb.com/docs/integration/http#ml-export

        Refer to: https://docs.surrealdb.com/docs/cli/ml/export
        """

    def _get_count(self, res) -> int:
        if not res:
            return 0
        if 'count' in res:
            return res['count']
        if 'result' in res:
            return self._get_count(res["result"])
        return self._get_count(res[0])
