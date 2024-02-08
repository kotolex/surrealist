from typing import List

from surrealist import Connection
from .delete_statements import DeleteUseWhere
from .statement import Statement
from ..utils import OK


class Delete(Statement, DeleteUseWhere):

    def __init__(self, connection: Connection, table_name: str):
        super().__init__(connection)
        self._table_name = table_name
        self._only = False

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        only = "" if not self._only else " ONLY"
        return f"DELETE{only} {self._table_name}"

    def only(self) -> "Delete":
        self._only = True
        return self
