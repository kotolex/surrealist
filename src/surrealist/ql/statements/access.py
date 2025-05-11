from typing import List

from surrealist.connections import Connection
from surrealist.utils import OK

from .access_statements import (CanUseGrant, CanUsePurge, CanUseRevoke,
                                CanUseShow)
from .statement import Statement


class Access(Statement, CanUseGrant, CanUseShow, CanUseRevoke, CanUsePurge):
    """
    Represents ACCESS statement, it should be able to use any statements from documentation

    Refer to: https://surrealdb.com/docs/surrealql/statements/access

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_access_examples.py

    ACCESS @name [ ON [ ROOT | NAMESPACE | DATABASE ] ] [
        GRANT [ FOR USER @name | FOR RECORD @record ]
        | SHOW [ GRANT @id | ALL | WHERE @expression ]
        | REVOKE [ GRANT @id | ALL | WHERE @expression ]
        | PURGE [ EXPIRED | REVOKED [ , EXPIRED | REVOKED ] ] [ FOR @duration ]
    ]
    """

    def __init__(self, connection: Connection, name: str):
        super().__init__(connection)
        self._name = name

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        return f"ACCESS {self._name} ON DATABASE"
