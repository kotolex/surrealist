import logging
from typing import Optional

from surrealist import Connection
from surrealist.ql.create import Create
from surrealist.ql.select import Select

logger = logging.getLogger("tableQL")


class Table:
    def __init__(self, name: str, connection: Connection):
        self._connected = connection.is_connected()
        self._connection = connection
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def is_connected(self):
        return self._connection.is_connected()

    def count(self) -> int:
        logger.info("Get records count for %s", self._name)
        return self._connection.count(self._name).result

    def select(self, *args) -> Select:
        return Select(self._connection, self.name, *args)

    def create(self, record_id: Optional[str] = None) -> Create:
        return Create(self._connection, self.name, record_id)
