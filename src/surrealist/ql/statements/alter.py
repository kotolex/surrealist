from typing import List

from surrealist.connections import Connection
from surrealist.utils import OK

from .permissions import CanUseComment, CanUsePermissions
from .statement import Statement


class Alter(Statement, CanUsePermissions, CanUseComment):
    """
    Represents ALTER statement, it should be able to use any statements from documentation

    Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/alter

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_alter_examples.py

    ALTER [
    | TABLE [ IF NOT EXISTS ] @name
    [ DROP ]
        [ SCHEMAFULL | SCHEMALESS ]
        [ PERMISSIONS [ NONE | FULL
            | FOR select @expression
            | FOR create @expression
            | FOR update @expression
            | FOR delete @expression
        ] ]
    [ COMMENT @string ]
    ]
    """

    def __init__(self, connection: Connection, name: str):
        super().__init__(connection)
        self._type = "TABLE"
        self._name = name
        self._if_not_exists = None
        self._drop = None
        self._schema_full = None
        self._as_select = None

    def validate(self) -> List[str]:
        return [OK]

    def if_not_exists(self) -> "Alter":
        """
        Represents IF NOT EXISTS statement
        """
        self._if_not_exists = True
        return self

    def drop(self) -> "Alter":
        """
        Represents DROP statement
        """
        self._drop = True
        return self

    def schemafull(self) -> "Alter":
        """
        Represents SCHEMAFULL statement
        """
        self._schema_full = True
        return self

    def schemaless(self) -> "Alter":
        """
        Represents SCHEMALESS statement
        """
        self._schema_full = False
        return self

    def _clean_str(self):
        exists = "" if not self._if_not_exists else " IF NOT EXISTS"
        drop = "" if not self._drop else " DROP"
        schema = ""
        if self._schema_full is True:
            schema = " SCHEMAFULL"
        elif self._schema_full is False:
            schema = " SCHEMALESS"
        return f"ALTER {self._type}{exists} {self._name}{drop}{schema}"
