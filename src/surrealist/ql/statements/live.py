from typing import List

from surrealist import Connection
from surrealist.ql.statements.live_statements import LiveUseWhere
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK


class Live(Statement, LiveUseWhere):

    def __init__(self, connection: Connection, table_name: str, use_diff: bool = False):
        super().__init__(connection)
        self._table_name = table_name
        self._alias = None
        self._diff = use_diff

    def alias(self, value_name: str, alias: str) -> "Live":
        self._alias = (value_name, alias)
        self._diff = False
        return self

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        if self._diff:
            what = "DIFF"
        elif self._alias:
            what = f"{self._alias[0]} AS {self._alias[1]}"
        else:
            what = "*"
        return f"LIVE SELECT {what} FROM {self._table_name}"
