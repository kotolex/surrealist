import json
from logging import getLogger
from typing import Tuple, Dict, Optional, Union, List, Callable, Any

from surrealist.errors import OperationOnClosedConnectionError, TooManyNestedLevelsError
from surrealist.utils import SurrealResult, DEFAULT_TIMEOUT

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
