from typing import Optional, List

from surrealist.connections.connection import Connection
from surrealist.enums import AutoOrNone
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK


class DefineConfig(Statement):
    """
    The DEFINE CONFIG GRAPHQL statement allows you to configure how your database’s tables and functions are exposed
    via the GraphQL API. This configuration is essential for enabling GraphQL functionality in your database,
    specifying which tables and functions should be included or excluded from the GraphQL schema.

    Refer to: https://surrealdb.com/docs/surrealql/statements/define/config

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/define_config.py

    DEFINE CONFIG [OVERWRITE | IF NOT EXISTS] GRAPHQL [ AUTO | NONE]
    [ TABLES (AUTO | NONE | INCLUDE table1, table2, ...) ]
    [ FUNCTIONS (AUTO | NONE | INCLUDE [function1, function2, ...] | EXCLUDE [function1, function2, ...]) ]
    """

    def validate(self) -> List[str]:
        return [OK]

    def __init__(self, connection: Connection, kind: Optional[AutoOrNone] = None):
        super().__init__(connection)
        self._if_not_exists = None
        self._auto = kind
        self._tables = None
        self._functions = None

    def if_not_exists(self) -> "DefineConfig":
        """
        Represents the IF NOT EXISTS statement.
        :return: DefineConfig object
        """
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineConfig":
        """
        Adds OVERWRITE statement to the query
        :return: DefineConfig object
        """
        self._if_not_exists = False
        return self

    def tables_kind(self, kind: AutoOrNone) -> "DefineConfig":
        """
        Represents the TABLES AUTO | NONE statement.
        :return: DefineConfig object
        """
        self._tables = kind
        return self

    def tables_include(self, raw_tables: str) -> "DefineConfig":
        """
        Represents the TABLES INCLUDE table1, table2, ... statement.
        :param raw_tables: raw string representation of tables, will be used as is
        :return: DefineConfig object
        """
        self._tables = f"INCLUDE {raw_tables}"
        return self

    def functions_kind(self, kind: AutoOrNone) -> "DefineConfig":
        """
        Represents the FUNCTIONS AUTO | NONE statement.
        :return: DefineConfig object
        """
        self._functions = kind
        return self

    def functions_include(self, raw_functions: str) -> "DefineConfig":
        """
        Represents the FUNCTIONS INCLUDE [function1, function2, ...] statement.
        :param raw_functions: raw string representation of functions, will be used as is
        :return: DefineConfig object
        """
        self._functions = f"INCLUDE {raw_functions}"
        return self

    def functions_exclude(self, raw_functions: str) -> "DefineConfig":
        """
        Represents the FUNCTIONS EXCLUDE [function1, function2, ...] statement.
        :param raw_functions: raw string representation of functions, will be used as is
        :return: DefineConfig object
        """
        self._functions = f"EXCLUDE {raw_functions}"
        return self

    def _clean_str(self):
        exists = ""
        if self._if_not_exists:
            exists = " IF NOT EXISTS"
        elif self._if_not_exists is False:
            exists = " OVERWRITE"
        auto = ""
        if self._auto == AutoOrNone.AUTO:
            auto = " AUTO"
        elif self._auto == AutoOrNone.NONE:
            auto = " NONE"
        tables = ""
        if self._tables is not None:
            if self._tables == AutoOrNone.AUTO:
                tables = " TABLES AUTO"
            elif self._tables == AutoOrNone.NONE:
                tables = " TABLES NONE"
            else:
                tables = f" TABLES {self._tables}"
        functions = ""
        if self._functions is not None:
            if self._functions == AutoOrNone.AUTO:
                functions = " FUNCTIONS AUTO"
            elif self._functions == AutoOrNone.NONE:
                functions = " FUNCTIONS NONE"
            else:
                functions = f" FUNCTIONS {self._functions}"

        return f"DEFINE CONFIG{exists} GRAPHQL{auto}{tables}{functions}"
