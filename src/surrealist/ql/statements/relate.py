from typing import List

from surrealist.connections import Connection
from surrealist.ql.statements.create_statements import CreateUseSetContent
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK


class Relate(Statement, CreateUseSetContent):
    """
    Represents RELATE statement, it should be able to use any statements from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/relate

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_relate_examples.py

    """

    def __init__(self, connection: Connection, value: str):
        """
        Init for Create
        :param connection: connection relied on
        :param value: relate representation, see examples
        """
        super().__init__(connection)
        self._only = False
        self._value = value

    def only(self) -> "Relate":
        """
        Include ONLY statement for the query
        """
        self._only = True
        return self

    def validate(self) -> List[str]:
        if "->" not in self._value and "<-" not in self._value:
            return ["Wrong format for RELATE representation"]
        return [OK]

    def _clean_str(self):
        only = "" if not self._only else " ONLY"
        return f"RELATE{only} {self._value}"
