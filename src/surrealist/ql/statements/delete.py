from typing import List, Optional

from surrealist.connections import Connection
from surrealist.utils import OK
from .common_statements import CanUseWhere
from .statement import Statement


class Delete(Statement, CanUseWhere):
    """
    Represents DELETE statement, it should be able to use any statements from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/delete

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_delete_examples.py
    """

    def __init__(self, connection: Connection, table_name: str, record_id: Optional[str] = None):
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
        """
        Include ONLY statement for the query
        """
        self._only = True
        return self
