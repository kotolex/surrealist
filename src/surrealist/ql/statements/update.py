from typing import List, Optional

from surrealist.connections import Connection
from surrealist.ql.statements.statement import Statement
from surrealist.ql.statements.update_statements import UpdateUseMethods
from surrealist.utils import OK, get_table_or_record_id


class Update(Statement, UpdateUseMethods):
    """
    Represents UPDATE statement, it should be able to use any statements from documentation
    The UPDATE statement can be used to update existing records in the database.
    If the record does not exist, the statement will fail and no records will be updated.

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/update

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_update_examples.py

    UPDATE [ ONLY ] @targets
    [ CONTENT @value
      | MERGE @value
      | PATCH @value
      | SET @field = @value ...
    ]
    [ WHERE @condition ]
    [ RETURN NONE | RETURN BEFORE | RETURN AFTER | RETURN DIFF | RETURN @statement_param, ... ]
    [ TIMEOUT @duration ]
    [ PARALLEL ];
    """

    def __init__(self, connection: Connection, table_name: str, record_id: Optional[str] = None):
        super().__init__(connection)
        self._table_name = table_name
        self._only = False
        self._record_id = record_id
        self._name = get_table_or_record_id(table_name, record_id)

    def validate(self) -> List[str]:
        return [OK]

    def only(self) -> "Update":
        """
        Include ONLY statement for the query
        """
        self._only = True
        return self

    def _clean_str(self):
        what = "" if not self._only else " ONLY"
        return f"UPDATE{what} {self._name}"
