import json
from abc import ABC, abstractmethod
from functools import wraps
from logging import getLogger
from typing import Tuple, Dict, Optional, Union, List, Callable, Any

from surrealist.errors import OperationOnClosedConnectionError, TooManyNestedLevelsError
from surrealist.result import SurrealResult
from surrealist.utils import DEFAULT_TIMEOUT, crop_data

logger = getLogger("connection")
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

    def _in_out_json(self, data, is_loads: bool):
        try:
            return json.loads(data) if is_loads else json.dumps(data)
        except RecursionError as e:
            logger.error("Cant serialize/deserialize object, too many nested levels")
            raise TooManyNestedLevelsError(f"Cant serialize object, too many nested levels\nRefer to: {LINK}") from e

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
        logger.info("Query-Operation: COUNT. Table: %s", crop_data(table_name))
        result = self.query(f"SELECT count() FROM {table_name} GROUP ALL;")
        if not result.is_error():
            result.result = self._get_count(result.result)
        return result

    @connected
    def table_info(self, table_name: str) -> SurrealResult:
        """
        Returns info about specified table. You should have permissions for this action.

        Actually converts to QL "INFO FOR TABLE table_name" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/info

        :return: full table information
        """
        return self._info(f"TABLE {table_name}")

    @connected
    def db_info(self) -> SurrealResult:
        """
        Returns info about a current database. You should have permissions for this action.

        Actually converts to QL "INFO FOR DB" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/info

        :return: full database information
        """
        return self._info("DB")

    @connected
    def ns_info(self) -> SurrealResult:
        """
        Returns info about current namespace. You should have permissions for this action.

        Actually converts to QL "INFO FOR NS" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/info

        :return: full namespace information
        """
        return self._info("NS")

    @connected
    def root_info(self) -> SurrealResult:
        """
        Returns info about root. You should have permissions for this action.

        Actually converts to QL "INFO FOR ROOT" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/info

        :return: information about root
        """
        return self._info("ROOT")

    def _info(self, type_: str) -> SurrealResult:
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
        query = 'return {"db" : session::db(), "session_id" : session::id(), "ip" : session::ip(), ' \
                '"ns" : session::ns(), "http_origin" : session::origin(), "scope" : session::sc()};'
        logger.info("Query-Operation: SESSION_INFO")
        return self.query(query)

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
    def remove_table(self, table_name: str) -> SurrealResult:
        """
        Fully removes table, even if it contains some records, analog of SQL "DROP table". You should have permissions
        for this action.This method cannot remove any other resource, if you need to remove db, ns or scope -
        use **query**

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/remove

        Note: only name of the table allowed here, do not use record_id

        :param table_name: name of the table
        :return: result of the query
        """
        logger.info("Query-Operation: REMOVE. Table name %s", table_name)
        return self.query(f"REMOVE TABLE {table_name};")

    @connected
    def show_changes(self, table_name: str, since: str, limit: int = 10) -> SurrealResult:
        """
        This method can use Changes Feed feature of SurrealDB. You need a predefined CHANGEFEED table and database for
        using this action.

        Refer to: https://surrealdb.com/blog/unlocking-streaming-data-magic-with-surrealdb-live-queries-and-change-feeds

        Refer to: https://github.com/kotolex/surrealist?tab=readme-ov-file#change-feeds

        :param table_name: name of the table, no record_id expected here
        :param since: str representation of ISO date-time, for example "2024-02-06T10:48:08.700483Z"
        :param limit: amount of changes to get
        :return: result of the query
        """
        query = f'SHOW CHANGES FOR TABLE {table_name} SINCE "{since}" LIMIT {limit};'
        return self.query(query)

    @abstractmethod
    def use(self, namespace: str, database: str) -> SurrealResult:
        """
        This method specifies the namespace and database for the current connection

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/use
        """

    @abstractmethod
    def authenticate(self, token: str) -> SurrealResult:
        """
        This method authenticates user with given token.
        Http connections cannot use it
        """

    @abstractmethod
    def invalidate(self) -> SurrealResult:
        """
        This method will invalidate the user's session for the current connection
        Http connections cannot use it
        """

    @abstractmethod
    def let(self, name: str, value) -> SurrealResult:
        """
        This method sets and stores a value which can then be used in a subsequent query

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/let
        """

    @abstractmethod
    def unset(self, name: str) -> SurrealResult:
        """
        This method unsets value, which was previously stored
        """

    @abstractmethod
    def live(self, table_name: str, callback: Callable[[Dict], Any], return_diff: bool = False) -> SurrealResult:
        """
        This method can be used to initiate live query - a real-time selection from a table. Works only for websockets.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/live-select

        About DIFF refer to: https://jsonpatch.com

        Please see surrealist documentation: https://github.com/kotolex/surrealist?tab=readme-ov-file#live-query
        """

    @abstractmethod
    def custom_live(self, custom_query: str, callback: Callable[[Dict], Any]) -> SurrealResult:
        """
        This method can be used to initiate custom live query - a real-time selection from a table with filters and
        other features of Live Query. Works only for websockets.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/live-select

        Please see surrealist documentation: https://github.com/kotolex/surrealist?tab=readme-ov-file#live-query

        Note: all results, DIFF, formats etc. should be specified in the query itself
        """

    @abstractmethod
    def kill(self, live_query_id: str) -> SurrealResult:
        """
        This method is used to terminate a running live query by id

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/kill
        """

    @abstractmethod
    def signup(self, namespace: str, database: str, scope: str, params: Optional[Dict] = None) -> SurrealResult:
        """
        This method allows you to sign up a user

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/scope
        """

    @abstractmethod
    def signin(self, user: str, password: str, namespace: Optional[str] = None,
               database: Optional[str] = None, scope: Optional[str] = None) -> SurrealResult:
        """
        This method allows you to sign in a root, namespace, database or scope user against SurrealDB
        """

    @abstractmethod
    def select(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method selects either all records in a table or a single record

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/select

        Notice: do not specify id twice: in table name and in record_id, it will cause error on SurrealDB side
        """

    @abstractmethod
    def create(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method creates a record either with a random or specified record_id. If no id specified in record_id or
        in data arguments, then id will be generated by SurrealDB

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/create

        Notice: do not specify id twice, for example, in table name and in data, it will cause error on SurrealDB side
        """

    @abstractmethod
    def insert(self, table_name: str, data: Union[Dict, List]) -> SurrealResult:
        """
        This method inserts one or more records. If you specify recordID in data and record with that id already
        exists - no inserts or updates will happen and the content of the existing record will be returned. If you need
        to change existing record, please consider **update** or **merge**

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/insert

        Note: do not use record id in table_name parameter (table:recordID) - it will cause error on SurrealDB side
        """

    @abstractmethod
    def update(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method can be used to update or create record in the database. So all old fields will be deleted, and new
        will be added, if you wand just to add field to record, keeping old ones -use **merge** method instead. If
        a record with specified id does not exist, it will be created, if exist - all fields will be replaced

        Note: if you want to create/replace one record, you should specify recordID in table_name or in record_id, but
        not in data parameters.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/update
        """

    @abstractmethod
    def merge(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method merges specified data into either all records in a table or a single record. Old fields in records
        will not be deleted if you want to replace old data with new - use **update** method. If a record with
        specified id does not exist, it will be created.
        """

    @abstractmethod
    def delete(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        """
        This method deletes all records in a table or a single record, be careful and don't forget to specify id if you
        do not want to delete all records. This method does not remove table itself, only records in it.
        As a result of this method, you will get all deleted records or None if no such record or table

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/delete
        """

    @abstractmethod
    def patch(self, table_name: str, data: Union[Dict, List], record_id: Optional[str] = None,
              return_diff: bool = False) -> SurrealResult:
        """
        This method changes specified data in one ar all records. If given table does not exist, new table and record
        will not be created if table exists but no such record_id - new record will be created, if no record id-all
        records will be transformed

        About allowed data format and DIFF refer to: https://jsonpatch.com

        Notice: do not specify id twice, for example, in table name and in record_id, it will cause error
        """

    @abstractmethod
    def query(self, query: str, variables: Optional[Dict] = None) -> SurrealResult:
        """
        This method used for execute a custom SurrealQL query

        For SurrealQL refer to: https://docs.surrealdb.com/docs/surrealql/overview
        """

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
        :return:
        """

    @abstractmethod
    def ml_import(self, path) -> SurrealResult:
        """
        This method imports a SurrealQL ML file into a local or remote SurrealDB database server.

        Note: websocket connection cannot use this method

        Refer to:
        https://docs.surrealdb.com/docs/integration/http#ml-import
        https://docs.surrealdb.com/docs/cli/ml/import
        :param path:
        :return:
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
