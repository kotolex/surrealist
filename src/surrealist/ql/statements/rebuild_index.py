from typing import List

from .statement import Statement
from surrealist.utils import OK
from surrealist.connections import Connection


class RebuildIndex(Statement):
    """
    Represents REBUILD statement, it should be able to use any statements from documentation

    Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/rebuild

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    """
    def __init__(self, connection: Connection, index_name: str, table_name: str, if_exists: bool = False):
        super().__init__(connection)
        self._name = index_name
        self._table_name = table_name
        self._if_exists = if_exists

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        exists = "" if not self._if_exists else " IF EXISTS"
        return f"REBUILD INDEX{exists} {self._name} ON TABLE {self._table_name}"