from logging import getLogger
from typing import Tuple, Dict, Optional, Union, List, Callable, Any

from py_surreal.errors import OperationOnClosedConnectionError
from py_surreal.utils import SurrealResult, DEFAULT_TIMEOUT

logger = getLogger("connection")


def connected(func):
    """
    Decorator for methods to make sure underlying connection is alive (connected to DB)

    :param func: method to decorate
    :raise OperationOnClosedConnectionError if connection is already closed
    """

    def wrapped(*args, **kwargs):
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
        :param timeout: timeout in second to wait connection results and responses
        """
        self.db_params = db_params
        self.credentials = credentials
        self.connected = False
        self.timeout = timeout

    def close(self):
        """
        Closes the connection. You can not and should not use connection object after that
        """
        logger.info("Connection was closed")
        self.connected = False

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()

    def is_connected(self) -> bool:
        """
        Returns if the connection is still alive and usable
        :return: True if connection is usable, False otherwise
        """
        return self.connected

    def use(self, namespace: str, database: str) -> SurrealResult:
        return NotImplemented

    def info(self) -> SurrealResult:
        return NotImplemented

    def is_ready(self) -> bool:
        return NotImplemented

    def status(self) -> str:
        return NotImplemented

    def health(self) -> str:
        return NotImplemented

    def version(self) -> str:
        return NotImplemented

    def export(self) -> str:
        return NotImplemented

    def select(self, table_name: str, record_id: Optional[str] = None) -> SurrealResult:
        return NotImplemented

    def ml_export(self, name: str, version: str) -> str:
        return NotImplemented

    def signin(self, user: str, password: str, namespace: Optional[str] = None,
               database: Optional[str] = None, scope: Optional[str] = None) -> SurrealResult:
        return NotImplemented

    def authenticate(self, token: str) -> SurrealResult:
        return NotImplemented

    def invalidate(self) -> SurrealResult:
        return NotImplemented

    def let(self, name: str, value) -> SurrealResult:
        return NotImplemented

    def unset(self, name: str) -> SurrealResult:
        return NotImplemented

    def live(self, table_name: str, callback: Callable[[Dict], Any], need_diff: bool = False) -> SurrealResult:
        return NotImplemented

    def kill(self, live_query_id: str) -> SurrealResult:
        return NotImplemented

    def signup(self, namespace: str, database: str, scope: str, params: Optional[Dict] = None) -> SurrealResult:
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

    def ml_import(self, path) -> SurrealResult:
        return NotImplemented
