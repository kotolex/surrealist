from typing import List, Optional

from surrealist import Connection
from surrealist.ql.statement import Statement
from surrealist.ql.update_statements import UpdateUseMethods
from surrealist.utils import OK


class Update(Statement, UpdateUseMethods):
    def __init__(self, connection: Connection, table_name: str, record_id: Optional[str] = None):
        super().__init__(connection)
        self._table_name = table_name
        self._only = False
        self._record_id = record_id

    def validate(self) -> List[str]:
        return [OK]

    def only(self) -> "Update":
        self._only = True
        return self

    def _clean_str(self):
        what = "" if not self._only else f" ONLY"
        table = self._table_name if not self._record_id else f"{self._table_name}:{self._record_id}"
        return f"UPDATE{what} {table}"
