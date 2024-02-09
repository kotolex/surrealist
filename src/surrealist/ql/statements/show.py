from typing import List

from surrealist import Connection
from surrealist.utils import OK
from .show_statements import ShowUseSince
from .statement import Statement


class Show(Statement, ShowUseSince):

    def __init__(self, connection: Connection, table_name: str):
        super().__init__(connection)
        self._table_name = table_name

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        return f"SHOW CHANGES FOR TABLE {self._table_name}"
