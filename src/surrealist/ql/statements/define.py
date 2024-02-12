from typing import List, Union, Any

from surrealist import Connection
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK


class DefineEvent(Statement):
    """
    Represents DEFINE EVENT statement

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
        Represents condition for event, WHEN statement

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
    Represents DEFINE USER statement

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
    Represents DEFINE PARAM statement

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
    Represents DEFINE ANALYZER statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/analyzer

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    """

    def __init__(self, connection: Connection, name: str):
        super().__init__(connection)
        self._name = name
        self._tokenizers = None
        self._filters = None

    def tokenizers(self, value: str) -> "DefineAnalyzer":
        """
        Represents TOKENIZERS statement
        """
        self._tokenizers = value
        return self

    def filters(self, value: str) -> "DefineAnalyzer":
        """
        Represents FILTERS statement
        """
        self._filters = value
        return self

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        tok = "" if not self._tokenizers else f" TOKENIZERS {self._tokenizers}"
        filters = "" if not self._filters else f" FILTERS {self._filters}"
        return f"DEFINE ANALYZER {self._name}{tok}{filters}"


class DefineScope(Statement):
    """
    Represents DEFINE SCOPE statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/scope

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    """

    def __init__(self, connection: Connection, name: str, duration: str, signup: Union[str, Statement],
                 signin: Union[str, Statement]):
        super().__init__(connection)
        self._name = name
        self._duration = duration
        self._signup = signup if not isinstance(signup, Statement) else f"({signup._clean_str()})"
        self._signin = signin if not isinstance(signin, Statement) else f"({signin._clean_str()})"

    def validate(self) -> List[str]:
        if ' ' in self._duration:
            return ["Wrong duration format, should be like 24h"]
        return [OK]

    def _clean_str(self):
        return f"DEFINE SCOPE {self._name} SESSION {self._duration} \nSIGNUP {self._signup} \nSIGNIN {self._signin}"


class DefineIndex(Statement):
    """
    Represents DEFINE INDEX statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/indexes

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    """

    def __init__(self, connection: Connection, name: str, table_name: str):
        super().__init__(connection)
        self._name = name
        self._table_name = table_name
        self._fields = None
        self._uni = False
        self._analyzer = None

    def fields(self, fields: str) -> "DefineIndex":
        """
        Represents fields for index, FIELDS statement

        :param fields: fields to index
        """
        self._fields = f"FIELDS {fields}"
        return self

    def columns(self, columns: str) -> "DefineIndex":
        """
        Represents fields for index, COLUMNS statement

        :param columns: fields to index
        """
        self._fields = f"COLUMNS {columns}"
        return self

    def unique(self) -> "DefineIndex":
        """
        Represents UNIQUE statement
        """
        self._analyzer = None
        self._uni = True
        return self

    def search_analyzer(self, name: str, use: str = "BM25", highlights: bool = True) -> "DefineIndex":
        """
        Represents SEARCH ANALYZER statement
        """
        self._uni = False
        hl = "" if not highlights else " HIGHLIGHTS"
        self._analyzer = f" SEARCH ANALYZER {name} {use}{hl}"
        return self

    def validate(self) -> List[str]:
        if not self._fields:
            return ["You need to specify FIELDS or COLUMNS"]
        return [OK]

    def _clean_str(self):
        add = ""
        if self._uni:
            add = " UNIQUE"
        elif self._analyzer:
            add = self._analyzer
        return f"DEFINE INDEX {self._name} ON TABLE {self._table_name} {self._fields}{add}"


class DefineToken(Statement):
    """
    Represents DEFINE TOKEN statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/token

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    """

    def __init__(self, connection: Connection, name: str, token_type: str, value: str):
        super().__init__(connection)
        self._name = name
        self._type = token_type
        self._value = value

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        return f'DEFINE TOKEN {self._name} ON DATABASE \nTYPE {self._type} \nVALUE "{self._value}"'
