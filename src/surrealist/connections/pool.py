from functools import wraps
from logging import getLogger
from os import cpu_count
from queue import Queue
from threading import Thread
from typing import Optional, Tuple, Dict, Callable, Any

from surrealist.connections.connection import Connection
from surrealist.enums import Transport
from surrealist.errors import OperationOnClosedConnectionError
from surrealist.result import SurrealResult
from surrealist.surreal import Surreal
from surrealist.utils import DEFAULT_TIMEOUT

CORES_COUNT = cpu_count()
logger = getLogger("surrealist.connection.pool")


def connected_and_pooled(func):
    """
    Wrapper to check if pool is connected ad delegate work to underlying connections
    :param func: function to wrap
    """

    @wraps(func)
    def wrapped(*args, **kwargs):
        # args[0] is a self-argument in methods
        if not args[0].is_connected():
            message = "Your pool is already closed"
            logger.error(message, exc_info=False)
            raise OperationOnClosedConnectionError(message)
        return args[0]._execute(func.__name__, *args[1:], **kwargs)

    return wrapped


class Pool:
    """
    Represents a pool of connections, which is creating a bunch of database connections on start and delegating all
    tasks to the first non-busy connection. So, if there are no more connections in the pool, it tries to create a new
    one if the maximum is not exceeded. If the maximum of connections is reached and no more connections to work with -
    client will be blocked until the first connection finishes the task and appears at the pool.
    """

    def __init__(self, first_connection: Connection, url: str, namespace: Optional[str] = None,
                 database: Optional[str] = None, access: Optional[str] = None, credentials: Tuple[str, str] = None,
                 use_http: bool = False, timeout: int = DEFAULT_TIMEOUT, min_connections: int = CORES_COUNT,
                 max_connections: int = 50):
        self._options = {
            "url": url, "namespace": namespace, "database": database, "access": access, "credentials": credentials,
            "use_http": use_http, "timeout": timeout
        }
        self._timeout = timeout
        self._url = url
        self._min = min_connections if min_connections > 1 else 2
        self._max = max_connections if max_connections <= 50 else 50
        self._main = Queue(maxsize=self._max)
        self._main.put_nowait(first_connection)
        self._counter = 1
        self._connected = True
        self._start()

    def transport(self) -> Transport:
        return Transport.HTTP if self._options["use_http"] else Transport.WEBSOCKET

    @property
    def connections_count(self) -> int:
        """
        Get the current number of connections, all of them can be busy now

        :return: number of connections in the pool
        """
        return self._counter

    def _start(self):
        for _ in range(self._min - 1):
            self._create_new_connection()
        logger.info("Created %s connections", self._min)

    def _create_new_connection(self):
        if self._counter < self._max:
            conn = Surreal(**self._options).connect()
            self._main.put_nowait(conn)
            self._counter += 1

    def close(self):
        """
        Closes the pool. You cannot and should not use a Pool object after that
        """
        self._connected = False
        logger.info("Signal to close pool")
        while not self._main.empty():
            # we need to gently wait for connection to finish the task
            conn = self._main.get(timeout=self._timeout)
            conn.close()
        logger.info("The Pool was closed")

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()

    def is_connected(self) -> bool:
        """
        Checks the pool is still alive and usable

        :return: True if pool is usable, False otherwise
        """
        return self._connected

    def _execute(self, name, *args, **kwargs) -> SurrealResult:
        """
        Here is the main "magic", this method checks if pool is empty (no more free connections) and if so - creates new
        connection. Then it waits until the first non-busy connection and delegates work to it, calling in method.
        After that - always put connection back to pool

        :param name: name of the connection method to call, for example, "query"
        :param args: args to call
        :param kwargs: keyword args to call
        :return: result of the query
        """
        if self._main.empty():
            Thread(target=self._create_new_connection, daemon=True).start()
        connection = self._main.get()
        try:
            result = getattr(connection, name)(*args, **kwargs)
        finally:
            self._main.put_nowait(connection)
        return result

    @connected_and_pooled
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

    @connected_and_pooled
    def db_info(self) -> SurrealResult:
        """
        Returns info about a current database. You should have permissions for this action.

        Actually converts to QL "INFO FOR DB" to use in **query** method.

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/info

        :return: full database information
        """

    @connected_and_pooled
    def db_tables(self) -> SurrealResult:
        """
        Returns all tables names in the current database. You should have permissions for this action.

        Actually call **db_info** and parse tables attribute there.

        :return: list of all tables names
        """

    @connected_and_pooled
    def custom_live(self, custom_query: str, callback: Callable[[Dict], Any]) -> SurrealResult:
        """
        This method can be used to initiate custom live query - a real-time selection from a table with filters and
        other features of Live Query. Works only for websockets.

        Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/live

        Please see surrealist documentation: https://github.com/kotolex/surrealist?tab=readme-ov-file#live-query

        Note: all results, DIFF, formats etc. should be specified in the query itself
        """

    @connected_and_pooled
    def kill(self, live_query_id: str) -> SurrealResult:
        """
        This method is used to terminate a running live query by id

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/kill
        """

    @connected_and_pooled
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
