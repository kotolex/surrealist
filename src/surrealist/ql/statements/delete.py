from typing import List, Optional

from surrealist import Connection
from .common_statements import CanUseWhere
from .statement import Statement
from surrealist.utils import OK


class Delete(Statement, CanUseWhere):

    def __init__(self, connection: Connection, table_name: str, record_id:Optional[str]=None):
        super().__init__(connection)
        self._table_name = table_name
        self._only = False
        self._record_id = record_id

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        only = "" if not self._only else " ONLY"
        name = self._table_name if not self._record_id else f"{self._table_name}:{self._record_id}"
        return f"DELETE{only} {name}"

    def only(self) -> "Delete":
        self._only = True
        return self
