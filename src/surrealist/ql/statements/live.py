from typing import List, Callable, Optional

from surrealist.connections import Connection
from surrealist.ql.statements.live_statements import LiveUseWhere
from surrealist.ql.statements.statement import Statement
from surrealist.result import SurrealResult
from surrealist.utils import OK


class Live(Statement, LiveUseWhere):
    """
    Represents LIVE SELECT statement, it should be able to use any statements from documentation

    Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/live

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_live_examples.py
    """

    def __init__(self, connection: Connection, table_name: str, callback: Callable, select: Optional[str] = None,
                 use_diff: bool = False):
        """
        Constructor for LIVE statement

        Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/live

        :param connection: connection to use
        :param table_name: name of the table to observe
        :param callback: function to call on incoming events
        :param select: raw query to insert between LIVE SELECT and FROM {table}, so the result will be
        LIVE SELECT {select} FROM {table_name}.
        If it is provided, other parameters (diff, alias, value) will be ignored
        :param use_diff: True if you want to get only DIFF info on table events, False for a standard results
        """
        super().__init__(connection)
        self._table_name = table_name
        self._alias = None
        self._diff = use_diff
        self._callback = callback
        self._select = select
        self._value = None

    def value(self, field_name: str) -> "Live":
        self._value = field_name
        return self

    def alias(self, value_name: str, alias: str) -> "Live":
        self._alias = (value_name, alias)
        self._diff = False
        return self

    def validate(self) -> List[str]:
        if self._select and any([self._value, self._diff, self._alias]):
            return ["If select is provided, value, diff and alias parameters will be ignored"]
        if self._value and any([self._diff, self._alias]):
            return ["Using Value with DIFF or alias parameters"]
        if self._alias and self._diff:
            return ["Using DIFF with alias parameter"]
        return [OK]

    def run(self) -> SurrealResult:
        return self._drill(self.to_str())

    def _drill(self, query):
        return self._connection.custom_live(query, self._callback)

    def _clean_str(self):
        what = "*"
        if self._value:
            what = f"VALUE {self._value}"
        if self._diff:
            what = "DIFF"
        elif self._alias:
            what = f"{self._alias[0]} AS {self._alias[1]}"
        if self._select:
            what = self._select
        return f"LIVE SELECT {what} FROM {self._table_name}"
