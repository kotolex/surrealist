import logging

from surrealist import Connection
from surrealist.errors import SurrealPermissionsError

logger = logging.getLogger("namespace")


class Namespace:
    def __init__(self, connection: Connection, namespace: str):
        self._connection = connection
        self._connected = True
        self._query = self._connection.query
        self._name = namespace
        result = self._connection.use(namespace)
        if result.is_error():
            self.close()
            logger.error("No permissions for %s, underlying result: %s", namespace, result)
            raise SurrealPermissionsError(f"You have no permissions for namespace {namespace}")

    def close(self):
        """
        Closes the connection. You can not and should not use namespace object after that
        """
        logger.info("Connection was closed")
        self._connected = False
        self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()

    def info(self):
        logger.info(f"Query-Operation: NS_INFO")
        return self._query("INFO FOR NS;")
