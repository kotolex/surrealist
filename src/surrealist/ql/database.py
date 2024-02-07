import logging
from typing import Optional, Tuple, List

from surrealist import Surreal, SurrealResult
from surrealist.ql.table import Table
from surrealist.utils import DEFAULT_TIMEOUT

logger = logging.getLogger("databaseQL")


class Database:
    def __init__(self, url: str, namespace: str, database: str, credentials: Optional[Tuple[str, str]],
                 use_http: bool = False, timeout: int = DEFAULT_TIMEOUT, log_level: str = "ERROR"):
        self._namespace = namespace
        self._database = database
        self._connection = Surreal(url, namespace, database, credentials, use_http, timeout, log_level).connect()
        self._connected = True
        logger.info("DatabaseQL is up")

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()

    def is_connected(self):
        return self._connected

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def database(self) -> str:
        return self._database

    def close(self):
        """
        Closes the connection. You cannot and should not use a connection object after that
        """
        logger.info("DatabaseQL is down")
        self._connection.close()
        self._connected = False

    def tables(self) -> List[str]:
        logger.info("Get tables for db %s", self._database)
        return self._connection.db_tables().result

    def info(self):
        logger.info("Get info for db %s", self._database)
        return self._connection.db_info().result

    def raw_query(self, query: str) -> SurrealResult:
        logger.info("Query for db %s", self._database)
        return self._connection.query(query)

    def __getattr__(self, item) -> Table:
        return Table(item, self._connection)

    def table(self, name) -> Table:
        return Table(name, self._connection)
