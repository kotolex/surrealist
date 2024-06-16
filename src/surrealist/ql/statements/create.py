from typing import List, Optional

from surrealist.connections import Connection
from surrealist.ql.statements.create_statements import CreateUseSetContent
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK

"""
CREATE [ ONLY ] @targets
    [ CONTENT @value
      | SET @field = @value ...
    ]
    [ RETURN NONE | RETURN BEFORE | RETURN AFTER | RETURN DIFF | RETURN @statement_param, ... ]
    [ TIMEOUT @duration ]
    [ PARALLEL ]
;
"""


class Create(Statement, CreateUseSetContent):
    """
    Represents CREATE statement, it should be able to use any statements from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/create

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_create_examples.py

    """

    def __init__(self, connection: Connection, table_name: str, record_id: Optional[str] = None):
        """
        Init for Create
        :param connection: connection relied on
        :param table_name: name of the table to operate
        :param record_id: optional, if exist-transform to 'table_name:record_id'
        """
        super().__init__(connection)
        self._table_name = table_name
        self._only = False
        self._record_id = record_id

    def only(self) -> "Create":
        """
        Include ONLY statement for the query
        """
        self._only = True
        return self

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        only = "" if not self._only else " ONLY"
        name = self._table_name if not self._record_id else f"{self._table_name}:{self._record_id}"
        return f"CREATE{only} {name}"
