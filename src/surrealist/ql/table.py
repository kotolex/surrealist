from typing import Optional

from surrealist import Connection, SurrealResult
from surrealist.ql.statements.create import Create
from surrealist.ql.statements.delete import Delete
from surrealist.ql.statements.insert import Insert
from surrealist.ql.statements.live import Live
from surrealist.ql.statements.remove import Remove
from surrealist.ql.statements.select import Select
from surrealist.ql.statements.show import Show
from surrealist.ql.statements.update import Update


class Table:
    def __init__(self, name: str, connection: Connection):
        self._connected = connection.is_connected()
        self._connection = connection
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def count(self) -> int:
        return self._connection.count(self._name).result

    def select(self, *args) -> Select:
        return Select(self._connection, self.name, *args)

    def create(self, record_id: Optional[str] = None) -> Create:
        return Create(self._connection, self.name, record_id)

    def show_changes(self) -> Show:
        return Show(self._connection, self._name)

    def delete(self, record_id: Optional[str] = None) -> Delete:
        return Delete(self._connection, self._name, record_id)

    def delete_all(self) -> SurrealResult:
        return Delete(self._connection, self._name).return_none().run()

    def drop(self) -> SurrealResult:
        return Remove(self._connection, self._name).run()

    def remove(self) -> SurrealResult:
        return self.drop()

    def live(self, use_diff: bool = False) -> Live:
        return Live(self._connection, self._name, use_diff)

    def kill(self, live_id: str) -> SurrealResult:
        return self._connection.kill(live_id)

    def insert_into(self, *args) -> Insert:
        return Insert(self._connection, self._name, *args)

    def update(self, record_id: Optional[str] = None) -> Update:
        return Update(self._connection, self._name, record_id)
