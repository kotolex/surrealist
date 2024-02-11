from typing import List, Union, Any

from surrealist import Connection
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK


class DefineEvent(Statement):
    """
    Represents DEFINE EVENT operator

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/event

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

    """

    def __init__(self, connection: Connection, name: str, table_name: str, then: Union[str, Statement]):
        super().__init__(connection)
        self._name = name
        self._table_name = table_name
        self._when = None
        self._then = then if not isinstance(then, Statement) else f"({then._clean_str()})"

    def when(self, predicate: str) -> "DefineEvent":
        """
        Represents condition for event, WHEN operator

        :param predicate: condition for event
        """
        self._when = predicate
        return self

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        when = f"WHEN {self._when} " if self._when else ""
        return f"DEFINE EVENT {self._name} ON TABLE {self._table_name} {when}THEN {self._then}"


class DefineUser(Statement):
    """
    Represents DEFINE USER operator

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/user

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    """

    def __init__(self, connection: Connection, user_name: str, password: str):
        super().__init__(connection)
        self._name = user_name
        self._pass = password
        self._role = "VIEWER"

    def role_owner(self) -> "DefineUser":
        """
        Represents role OWNER

        """
        self._role = "OWNER"
        return self

    def role_editor(self) -> "DefineUser":
        """
        Represents role EDITOR

        """
        self._role = "EDITOR"
        return self

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        return f"DEFINE USER {self._name} ON DATABASE PASSWORD '{self._pass}' ROLES {self._role}"


class DefineParam(Statement):
    """
    Represents DEFINE PARAM operator

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/param

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    """

    def __init__(self, connection: Connection, param_name: str, value: Any):
        super().__init__(connection)
        self._name = param_name
        self._value = value

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        return f"DEFINE PARAM ${self._name} VALUE {self._value}"


class DefineAnalyzer(Statement):
    """
    Represents DEFINE ANALYZER operator

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/analyzer

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    """

    # DEFINE ANALYZER @name [ TOKENIZERS @tokenizers ] [ FILTERS @filters ]
    def __init__(self, connection: Connection, name: str):
        super().__init__(connection)
        self._name = name
        self._tokenizers = None
        self._filters = None

    def tokenizers(self, value: str) -> "DefineAnalyzer":
        self._tokenizers = value
        return self

    def filters(self, value: str) -> "DefineAnalyzer":
        self._filters = value
        return self

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        tok = "" if not self._tokenizers else f" TOKENIZERS {self._tokenizers}"
        filters = "" if not self._filters else f" FILTERS {self._filters}"
        return f"DEFINE ANALYZER {self._name}{tok}{filters}"
