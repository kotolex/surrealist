import json
from logging import getLogger
from typing import Tuple, Dict, Optional, Union, List, Callable, Any

from surrealist.errors import OperationOnClosedConnectionError, TooManyNestedLevelsError
from surrealist.utils import SurrealResult, DEFAULT_TIMEOUT, crop_data

logger = getLogger("connection")
LINK = "https://github.com/kotolex/py_surreal?tab=readme-ov-file#recursion-and-json-in-python"


def connected(func):
    """
    Decorator for methods to make sure underlying connection is alive (connected to DB)

    :param func: method to decorate
    :raise OperationOnClosedConnectionError: if connection is already closed
    """

    def wrapped(*args, **kwargs):
        # args[0] is a self argument in methods
        if not args[0].is_connected():
            message = "Your connection already closed"
            logger.error(message, exc_info=False)
            raise OperationOnClosedConnectionError(message)
        return func(*args, **kwargs)

    return wrapped


class Connection:
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
        Closes the connection. You can not and should not use connection object after that
        """
        logger.info("Connection was closed")
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

    def count(self, table_name: str) -> SurrealResult:
        """
        Returns records count for given table. You should have permissions for this action.
        Actually converts to QL "SELECT count() FROM {table_name} GROUP ALL;" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/functions/count

        Note: returns 0 if table not exists, if you need to check table existence use **is_table_exists**

        Note: if you specify table_name with recordID like "person:john" you will get count of fields in record

        :param table_name: name of the table
        :return: result containing count, like SurrealResult(id='', error=None, result=[{'count': 1}], time='123.333µs')
        """
        logger.info("Query-Operation: COUNT. Table: %s", crop_data(table_name))
        result = self.query(f"SELECT count() FROM {table_name} GROUP ALL;")
        if not result.is_error():
            result.result = self._get_count(result.result)
        return result

    def db_info(self) -> SurrealResult:
        """
        Returns info about current database. You should have permissions for this action.

        Actually converts to QL "INFO FOR DB;" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/info

        :return: full db information
        """
        logger.info("Query-Operation: DB_INFO")
        return self.query("INFO FOR DB;")

    def session_info(self) -> SurrealResult:
        """
        Returns info about current session. You should have permissions for this action.

        Actually converts to QL query to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/functions/session

        :return: full session information
        """
        query = """return {"db" : session::db(), "session_id" : session::id(), "ip" : session::ip(), 
        "ns" : session::ns(), "http_origin" : session::origin(), "scope" : session::sc()};"""
        logger.info("Query-Operation: SESSION_INFO")
        return self.query(query)

    def db_tables(self) -> SurrealResult:
        """
        Returns all tables names in current database. You should have permissions for this action.

        Actually call **db_info** and parse tables attribute there.

        :return: list of all tables names
        """
        logger.info("Query-Operation: DB_TABLES")
        res = self.db_info()
        res.result = list(res.result["tables"].keys())
        return res

    def remove_table(self, table_name: str) ->SurrealResult:
        """
        Fully removes table, even if it contains some records, analog of SQL "DROP table". You should have permissions
        for this action.This method can not remove any other resource, if you need to remove db, ns or scope -
        use **query**

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/remove

        Note: only name of the table allowed here, do not use record_id

        :param table_name: name of the table
        :return: result of the query
        """
        logger.info("Query-Operation: REMOVE. Table name %s", table_name)
        return self.query(f"REMOVE TABLE {table_name};")


    def use(self, namespace: str, database: str) -> SurrealResult:
        return NotImplemented

    def info(self) -> SurrealResult:
        return NotImplemented

    def authenticate(self, token: str) -> SurrealResult:
        return NotImplemented

    def invalidate(self) -> SurrealResult:
        return NotImplemented

    def let(self, name: str, value) -> SurrealResult:
        return NotImplemented

    def unset(self, name: str) -> SurrealResult:
        return NotImplemented

    def live(self, table_name: str, callback: Callable[[Dict], Any], return_diff: bool = False) -> SurrealResult:
        return NotImplemented

    def kill(self, live_query_id: str) -> SurrealResult:
        return NotImplemented

    def signup(self, namespace: str, database: str, scope: str, params: Optional[Dict] = None) -> SurrealResult:
        return NotImplemented

    def signin(self, user: str, password: str, namespace: Optional[str] = None,
               database: Optional[str] = None, scope: Optional[str] = None) -> SurrealResult:
        return NotImplemented

    def select(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        return NotImplemented

    def create(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        return NotImplemented

    def insert(self, table_name: str, data: Union[Dict, List]) -> SurrealResult:
        return NotImplemented

    def update(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        return NotImplemented

    def merge(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        return NotImplemented

    def delete(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        return NotImplemented

    def patch(self, table_name: str, data: Union[Dict, List], record_id: Optional[str] = None,
              return_diff: bool = False) -> SurrealResult:
        return NotImplemented

    def query(self, query: str, variables: Optional[Dict] = None) -> SurrealResult:
        return NotImplemented

    def import_data(self, path) -> SurrealResult:
        return NotImplemented

    def export(self) -> str:
        return NotImplemented

    def ml_import(self, path) -> SurrealResult:
        return NotImplemented

    def ml_export(self, name: str, version: str) -> str:
        return NotImplemented

    def _get_count(self, res) -> int:
        if not res:
            return 0
        if 'count' in res:
            return res['count']
        if 'result' in res:
            return self._get_count(res["result"])
        return self._get_count(res[0])
