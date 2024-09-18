import logging
from os import cpu_count
from typing import Optional, Tuple

from surrealist.connections.pool import Pool
from surrealist.ql.database import Database
from surrealist.utils import DEFAULT_TIMEOUT

CORES_COUNT = cpu_count()
logger = logging.getLogger("surrealist.databaseQL.pool")


class DatabaseConnectionsPool(Database):
    """
    Represents a pool of connections to a database, for using in high-load multithreading environments. As a child of
    Database object it can be used everywhere where Database can.
    The main difference is a number of created connections. By default, the minimum number is equal to CPU cores count
    in the system and cannot be less than 2.

    During work, if no more non-busy connections to use, then new connection will be created until its number not
    reaches maximum for the pool

    Refer to: https://github.com/kotolex/surrealist?tab=readme-ov-file#connections-pool

    """

    def __init__(self, url: str, namespace: str, database: str, access: Optional[str] = None,
                 credentials: Optional[Tuple[str, str]] = None,
                 use_http: bool = False, timeout: int = DEFAULT_TIMEOUT,
                 min_connections: int = CORES_COUNT, max_connections: int = 50):
        """
        All parameters are the same as for Surreal or Database object

        :param min_connections: minimum number of connections, it cannot be less than 2
        :param max_connections: maximum number of connections, it cannot be more than 50
        """
        self._options = {
            "url": url, "namespace": namespace, "database": database, "access": access, "credentials": credentials,
            "use_http": use_http, "timeout": timeout, "min_connections": min_connections,
            "max_connections": max_connections
        }
        super().__init__(url, namespace, database, access, credentials, use_http, timeout)
        self._connection = Pool(self._connection, **self._options)
        self._connected = True
        self._min = min_connections
        self._max = max_connections
        logger.info("Pool DatabaseQL is up")

    @property
    def connections_count(self) -> int:
        """
        Get the current number of connections, all of them can be busy now

        :return: number of connections in the pool
        """
        return self._connection.connections_count

    @property
    def min_connections(self) -> int:
        """
        Returns minimum number of connections for current pool

        :return: minimum number of connections in the pool
        """
        return self._min

    @property
    def max_connections(self) -> int:
        """
        Returns maximum number of connections for current pool

        :return: maximum number of connections in the pool
        """
        return self._max

    def __repr__(self):
        return f"DatabasePool(namespace={self._namespace}, name={self._database}, connected={self.is_connected()}," \
               f"connections_count={self.connections_count}, min_connections={self._min}, max_connections={self._max})"
