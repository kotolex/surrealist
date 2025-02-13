from typing import List

from surrealist.connections.connection import Connection
from surrealist.ql.statements.define import Define
from surrealist.ql.statements.permissions import CanUsePermissions
from surrealist.utils import OK


class DefineField(Define, CanUsePermissions):
    """
    Represents DEFINE FIELD statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/field
    Refer to: https://surrealdb.com/docs/surrealql/datamodel/references

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/define_field.py

    DEFINE FIELD [ OVERWRITE | IF NOT EXISTS ] @name ON [ TABLE ] @table
        [ [ FLEXIBLE ] TYPE @type ]
        [ REFERENCE
            [   ON DELETE REJECT |
                ON DELETE CASCADE |
                ON DELETE IGNORE |
                ON DELETE UNSET |
                ON DELETE THEN @expression ]
        ]
        [ DEFAULT [ALWAYS] @expression ]
      [ READONLY ]
        [ VALUE @expression ]
        [ ASSERT @expression ]
        [ PERMISSIONS [ NONE | FULL
            | FOR select @expression
            | FOR create @expression
            | FOR update @expression
        ] ]
      [ COMMENT @string ]
    """

    def __init__(self, connection: Connection, field_name: str, table_name: str):
        super().__init__(connection)
        self._field_name = field_name
        self._table_name = table_name
        self._flex = False
        self._type = None
        self._default = None
        self._readonly = False
        self._value = None
        self._assert = None
        self._ref = None

    def if_not_exists(self) -> "DefineField":
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineField":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

    def type(self, type_name: str, is_flexible: bool = False) -> "DefineField":
        """
        Represents TYPE statement and FLEXIBLE statement
        """
        self._flex = is_flexible
        self._type = type_name
        return self

    def default(self, value: str, always: bool = False) -> "DefineField":
        """
        Represents DEFAULT statement
        """
        self._default = (always, value)
        return self

    def read_only(self) -> "DefineField":
        """
        Represents READONLY statement
        """
        self._readonly = True
        return self

    def value(self, value: str) -> "DefineField":
        """
        Represents VALUE statement
        """
        self._value = value
        return self

    def asserts(self, value: str) -> "DefineField":
        """
        Represents ASSERT statement
        """
        self._assert = value
        return self

    def reference(self) -> "DefineField":
        """
        Represents only REFERENCE statement
        """
        self._ref = "REFERENCE"
        return self

    def reference_reject(self) -> "DefineField":
        """
        Represents REFERENCE ON DELETE REJECT statement
        """
        self._ref = "REFERENCE ON DELETE REJECT"
        return self

    def reference_cascade(self) -> "DefineField":
        """
        Represents REFERENCE ON DELETE CASCADE statement
        """
        self._ref = "REFERENCE ON DELETE CASCADE"
        return self

    def reference_ignore(self) -> "DefineField":
        """
        Represents REFERENCE ON DELETE IGNORE statement
        """
        self._ref = "REFERENCE ON DELETE IGNORE"
        return self

    def reference_unset(self) -> "DefineField":
        """
        Represents REFERENCE ON DELETE UNSET statement
        """
        self._ref = "REFERENCE ON DELETE UNSET"
        return self

    def reference_then(self, value: str) -> "DefineField":
        """
        Represents REFERENCE ON DELETE THEN @expression statement
        """
        self._ref = f"REFERENCE ON DELETE THEN {value}"
        return self

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        type_ = "" if not self._type else f" TYPE {self._type}"
        if type_ and self._flex:
            type_ = f" FLEXIBLE{type_}"
        default = ""
        if self._default:
            default = f" DEFAULT {self._default[1]}" if not self._default[0] else f" DEFAULT ALWAYS {self._default[1]}"
        ro = "" if not self._readonly else " READONLY"
        value = "" if not self._value else f" VALUE {self._value}"
        asserts = "" if not self._assert else f" ASSERT {self._assert}"
        ref = "" if not self._ref else f" {self._ref}"
        return f'DEFINE FIELD{self._exists()} {self._field_name} ' \
               f'ON TABLE {self._table_name}{type_}{ref}{default}{ro}{value}{asserts}{self._comment()}'
