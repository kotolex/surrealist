from typing import List, Optional

from surrealist import Connection
from surrealist.ql.statements.statement import Statement
from surrealist.ql.statements.update_statements import UpdateUseMethods
from surrealist.utils import OK


class Update(Statement, UpdateUseMethods):
    """
    Represents UPDATE operator, it should be able to use any operators from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/update

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_update_examples.py
    """
    def __init__(self, connection: Connection, table_name: str, record_id: Optional[str] = None):
        super().__init__(connection)
        self._table_name = table_name
        self._only = False
        self._record_id = record_id

    def validate(self) -> List[str]:
        return [OK]

    def only(self) -> "Update":
        """
        Include ONLY operator for the query
        """
        self._only = True
        return self

    def _clean_str(self):
        what = "" if not self._only else " ONLY"
        table = self._table_name if not self._record_id else f"{self._table_name}:{self._record_id}"
        return f"UPDATE{what} {table}"
