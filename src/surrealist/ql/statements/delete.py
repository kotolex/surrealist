from typing import List, Optional

from surrealist.connections import Connection
from surrealist.utils import OK, StrOrRecord, get_table_or_record_id
from .common_statements import CanUseWhere
from .statement import Statement


class Delete(Statement, CanUseWhere):
    """
    Represents DELETE statement, it should be able to use any statements from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/delete

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_delete_examples.py

    DELETE [ ONLY ] @targets
    [ WHERE @condition ]
    [ RETURN NONE | RETURN BEFORE | RETURN AFTER | RETURN DIFF | RETURN @statement_param, ... ]
    [ TIMEOUT @duration ]
    [ PARALLEL ];
    """

    def __init__(self, connection: Connection, table_name: str, record_id: Optional[StrOrRecord] = None):
        super().__init__(connection)
        self._table_name = table_name
        self._only = False
        self._record_id = record_id
        self._name = get_table_or_record_id(table_name, record_id)

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        only = "" if not self._only else " ONLY"
        return f"DELETE{only} {self._name}"

    def only(self) -> "Delete":
        """
        Include ONLY statement for the query
        """
        self._only = True
        return self
