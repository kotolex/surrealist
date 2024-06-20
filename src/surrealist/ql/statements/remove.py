from typing import List, Optional

from surrealist.connections import Connection
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK

"""
REMOVE [
    NAMESPACE [ IF EXISTS ] @name
    | DATABASE [ IF EXISTS] @name
    | USER [ IF EXISTS ] @name ON [ ROOT | NAMESPACE | DATABASE ]
    | ACCESS [ IF EXISTS ] @name ON [ NAMESPACE | DATABASE ]
    | EVENT [ IF EXISTS ] @name ON [ TABLE ] @table
    | FIELD [ IF EXISTS ] @name ON [ TABLE ] @table
    | INDEX [ IF EXISTS ] @name ON [ TABLE ] @table
    | ANALYZER [ IF EXISTS ] @name
    | FUNCTION [ IF EXISTS ] fn::@name
    | PARAM [ IF EXISTS ] $@name
    | TABLE [ IF EXISTS ] @name
]
"""


class Remove(Statement):
    """
    Represents REMOVE statement, it should be able to use any statements from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/remove

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    """
    _variants = ("TABLE", "EVENT", "FIELD", "INDEX", "PARAM", "USER", "ANALYZER", "SCOPE", "TOKEN", "ACCESS")

    def __init__(self, connection: Connection, table_name: str, type_: str = "TABLE", name: Optional[str] = None):
        if type_ not in Remove._variants:
            raise ValueError(f"Type should be one of {Remove._variants}")
        super().__init__(connection)
        self._table_name = table_name
        self._only = False
        self._type = type_
        self._name = name
        self._on_exists = False
        if type_ == "TABLE":
            self._name = table_name

    def validate(self) -> List[str]:
        return [OK]

    def if_exists(self) -> "Remove":
        """
        Adds IF EXISTS to a final statement

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/remove#using-if-exists-clause-since-130

        :return: Remove object
        """
        self._on_exists = True
        return self

    def _clean_str(self):
        add = "" if not self._on_exists else " IF EXISTS"
        if self._type in ("TABLE", "PARAM", "USER", "ANALYZER", "SCOPE", "TOKEN", "ACCESS"):
            what = f"{self._type}{add} {self._name}"
            if self._type in ("USER", "TOKEN", "ACCESS"):
                what = f"{what} ON DATABASE"
            if self._type == "PARAM":
                what = f"{self._type}{add} ${self._name}"
        else:
            what = f"{self._type}{add} {self._name} ON TABLE {self._table_name}"
        return f"REMOVE {what}"
