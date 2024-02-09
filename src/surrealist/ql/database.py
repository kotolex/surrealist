import logging
from typing import Optional, Tuple, List, Dict

from surrealist import Surreal, SurrealResult
from surrealist.ql.table import Table
from surrealist.utils import DEFAULT_TIMEOUT

logger = logging.getLogger("databaseQL")


class Database:
    """
    Represents connected database(in some namespace) to operate on.
    It has features of the database level, including switch to table level. You can use DEFINE/REMOVE, query or
    transactions here, but for simple CRUD or live queries you need to switch to table level
    """

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

    def is_connected(self) -> bool:
        """
        Return if a database is connected
        :return: True if connection is active, False otherwise
        """
        return self._connected

    @property
    def namespace(self) -> str:
        """
        Return name if the namespace
        :return: namespace
        """
        return self._namespace

    @property
    def name(self) -> str:
        """
        Return name of the database
        :return: database name
        """
        return self._database

    def close(self):
        """
        Closes the connection. You cannot and should not use a database object after that
        """
        logger.info("DatabaseQL is down")
        self._connection.close()
        self._connected = False

    def tables(self) -> List[str]:
        """
        Return list of the table names at a current database
        :return: string list with the names
        """
        logger.info("Get tables for db %s", self._database)
        return self._connection.db_tables().result

    def info(self) -> Dict:
        """
        Return full info about a database, actually call for "INFO FOR DB;" via query
        :return: a result of the response
        """
        logger.info("Get info for db %s", self._database)
        return self._connection.db_info().result

    def raw_query(self, query: str) -> SurrealResult:
        """
        Execute a raw QL query on a database, you should use it in cases when your query is large or complicated, or it
        is just more appropriate for you.

        :param query: text of the query, it will be sent to SurrealDB as is
        :return: a result of the response
        """
        logger.info("Query for db %s", self._database)
        return self._connection.query(query)

    def __getattr__(self, item) -> Table:
        """
        We actually need this to use dot as the switch to table level as "db.person" instead of "db.table("person")"
        :param item: name of the table to work with
        :return: a table object
        """
        return Table(item, self._connection)

    def table(self, name) -> Table:
        """
        Switch to the Table level(an object) after that you can and should use table operations (CRUD)

        :param name: name of the table to work with
        :return: a table object
        """
        return Table(name, self._connection)

    def __repr__(self):
        return f"Database(namespace={self._namespace}, name={self._database}, connected={self.is_connected()})"
