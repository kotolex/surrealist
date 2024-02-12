from typing import List

from surrealist.connections import Connection
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK


class Return(Statement):
    """
    Represent RETURN statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/return

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

    """

    def __init__(self, connection: Connection, query: str):
        """
        Init for Create
        :param connection: connection relied on
        :param query: raw query to return
        """
        super().__init__(connection)
        self._query = query

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        return f"RETURN {self._query}"
