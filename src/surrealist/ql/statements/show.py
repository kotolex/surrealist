from typing import List

from surrealist import Connection
from surrealist.utils import OK
from .show_statements import ShowUseSince
from .statement import Statement


class Show(Statement, ShowUseSince):
    """
    Represents SHOW CHANGES operator, it should be able to use any operators from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/show

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_show_examples.py
    """

    def __init__(self, connection: Connection, table_name: str):
        super().__init__(connection)
        self._table_name = table_name

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        return f"SHOW CHANGES FOR TABLE {self._table_name}"
