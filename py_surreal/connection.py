from typing import Tuple, Dict, Optional, Union, List

from py_surreal.utils import SurrealResult, DEFAULT_TIMEOUT
from py_surreal.errors import OperationOnClosedConnectionError


def connected(func):
    def wrapped(*args, **kwargs):
        if not args[0].is_connected():
            raise OperationOnClosedConnectionError("Your connection already closed")
        return func(*args, **kwargs)

    return wrapped


class Connection:
    def __init__(self, db_params: Optional[Dict] = None, credentials: Tuple[str, str] = None,
                 timeout: int = DEFAULT_TIMEOUT):
        self.db_params = db_params
        self.credentials = credentials
        self.connected = False
        self.timeout = timeout

    def close(self):
        self.connected = False

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()

    def is_connected(self) -> bool:
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

    def kill(self, live_query_id: str) -> SurrealResult:
        return NotImplemented

    def signup(self, namespace: str, database: str, scope: str, params: Optional[Dict] = None) -> SurrealResult:
        return NotImplemented

    def create(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> SurrealResult:
        return NotImplemented

    def insert(self, table_name: str, data: Dict) -> SurrealResult:
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
