from typing import List, Optional

from surrealist.connections import Connection
from surrealist.ql.statements.create_statements import CreateUseSetContent
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK, StrOrRecord, get_table_or_record_id


class Create(Statement, CreateUseSetContent):
    """
    Represents CREATE statement, it should be able to use any statements from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/create

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_create_examples.py

    CREATE [ ONLY ] @targets
    [ CONTENT @value
      | SET @field = @value ...
    ]
    [ RETURN NONE | RETURN BEFORE | RETURN AFTER | RETURN DIFF | RETURN @statement_param, ... ]
    [ TIMEOUT @duration ]
    [ PARALLEL ];
    """

    def __init__(self, connection: Connection, table_name: str, record_id: Optional[StrOrRecord] = None):
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
        self._name = get_table_or_record_id(self._table_name, record_id)

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
        return f"CREATE{only} {self._name}"
