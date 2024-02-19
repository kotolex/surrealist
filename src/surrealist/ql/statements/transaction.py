from typing import List

from surrealist.connections import Connection
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK


class Transaction(Statement):
    """
    Represents TRANSACTION operation, it generates and can run a transaction query

    Refer to: https://docs.surrealdb.com/docs/surrealql/transactions

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/transaction.py

    """

    def __init__(self, connection: Connection, actions: List[Statement]):
        super().__init__(connection)
        self._actions = actions

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self) -> str:
        text = "BEGIN TRANSACTION;"
        between = "\n".join(statement.to_str() for statement in self._actions)
        end = "COMMIT TRANSACTION"
        return f"{text}\n\n{between}\n\n{end}"
