import json
from abc import ABC
from typing import List, Union, Any, Optional, Tuple

from surrealist.connections import Connection
from surrealist.enums import Algorithm
from surrealist.ql.statements.define_index_statements import CanUseIndexTypes
from surrealist.ql.statements.permissions import CanUsePermissions
from surrealist.ql.statements.statement import Statement, FinishedStatement
from surrealist.utils import OK


class Define(Statement, ABC):
    """
    Parent for all DEFINE statements, can use IF NOT EXISTS statement
    """

    def __init__(self, connection: Connection):
        super().__init__(connection)
        self._if_not_exists = None
        self._text = None

    def _exists(self) -> str:
        if self._if_not_exists is None:
            return ""
        return " IF NOT EXISTS" if self._if_not_exists else " OVERWRITE"

    def _comment(self) -> str:
        return f" COMMENT {json.dumps(self._text)}" if self._text else ""

    def if_not_exists(self) -> "Define":
        """
        Adds IF NOT EXISTS statement to the query
        :return: self
        """
        self._if_not_exists = True
        return self

    def overwrite(self) -> "Define":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

    def comment(self, comment: str) -> FinishedStatement:
        """
        Adds COMMENT statement to the query
        :param comment: comment text
        :return: self
        """
        self._text = comment
        return FinishedStatement(self)


class DefineEvent(Define):
    """
    Represents DEFINE EVENT statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/event

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

    DEFINE EVENT [ OVERWRITE | IF NOT EXISTS ] @name ON [ TABLE ] @table [ WHEN @expression ] THEN @expression
    [ COMMENT @string ]

    """

    def __init__(self, connection: Connection, name: str, table_name: str, then: Union[str, Statement]):
        super().__init__(connection)
        self._name = name
        self._table_name = table_name
        self._when = None
        self._then = then if not isinstance(then, Statement) else f"({then._clean_str()})"

    def if_not_exists(self) -> "DefineEvent":
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineEvent":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

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
        return f"DEFINE EVENT{self._exists()} {self._name} ON TABLE {self._table_name} {when}" \
               f"THEN {self._then}{self._comment()}"


class DefineParam(Define):
    """
    Represents DEFINE PARAM statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/param

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

    DEFINE PARAM [ OVERWRITE | IF NOT EXISTS ] $@name VALUE @value [ COMMENT @string ]
    """

    def __init__(self, connection: Connection, param_name: str, value: Any):
        super().__init__(connection)
        self._name = param_name
        self._value = value

    def if_not_exists(self) -> "DefineParam":
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineParam":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        return f"DEFINE PARAM{self._exists()} ${self._name} VALUE {self._value}{self._comment()}"


class DefineScope(Define):
    """
    Deprecated since SurrealDB 2.x, use define_access_record instead!

    Represents DEFINE SCOPE statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/scope

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

    DEFINE SCOPE [ OVERWRITE | IF NOT EXISTS ] @name SESSION @duration SIGNUP @expression SIGNIN @expression
    [ COMMENT @string ]
    """

    def __init__(self, connection: Connection, name: str, duration: str, signup: Union[str, Statement],
                 signin: Union[str, Statement]):
        super().__init__(connection)
        self._name = name
        self._duration = duration
        self._signup = signup if not isinstance(signup, Statement) else f"({signup._clean_str()})"
        self._signin = signin if not isinstance(signin, Statement) else f"({signin._clean_str()})"

    def if_not_exists(self) -> "DefineScope":
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineScope":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

    def validate(self) -> List[str]:
        if ' ' in self._duration:
            return ["Wrong duration format, should be like 24h"]
        return [OK]

    def _clean_str(self):
        return f"DEFINE SCOPE{self._exists()} {self._name} SESSION {self._duration} \nSIGNUP {self._signup} " \
               f"\nSIGNIN {self._signin}{self._comment()}"


class DefineIndex(Define, CanUseIndexTypes):
    """
    Represents DEFINE INDEX statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/indexes

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

    DEFINE INDEX [ OVERWRITE | IF NOT EXISTS ] @name ON [ TABLE ] @table [ FIELDS | COLUMNS ] @fields
    [ UNIQUE
        | SEARCH ANALYZER @analyzer [ BM25 [(@k1, @b)] ] [ HIGHLIGHTS ]
        | MTREE DIMENSION @dimension [ TYPE @type ] [ DIST @distance ] [ CAPACITY @capacity]
        | HNSW DIMENSION @dimension [ TYPE @type ] [DIST @distance] [ EFC @efc ] [ M @m ]
    ]
    [ COMMENT @string ]
    """

    def __init__(self, connection: Connection, name: str, table_name: str):
        super().__init__(connection)
        self._name = name
        self._table_name = table_name
        self._fields = None
        self._uni = False
        self._analyzer = None

    def if_not_exists(self) -> "DefineIndex":
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineIndex":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

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

    def validate(self) -> List[str]:
        if not self._fields:
            return ["You need to specify FIELDS or COLUMNS"]
        return [OK]

    def _clean_str(self):
        return f"DEFINE INDEX{self._exists()} {self._name} ON TABLE {self._table_name} {self._fields}{self._comment()}"


class DefineToken(Define):
    """
    Deprecated since SurrealDB 2.x, use define_access_jwt or define_access_record instead!

    Represents DEFINE TOKEN statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/token

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

    DEFINE TOKEN [ OVERWRITE | IF NOT EXISTS ] @name ON [ NAMESPACE | DATABASE | SCOPE @scope ] TYPE @type VALUE @value
    [ COMMENT @string ]
    """

    def __init__(self, connection: Connection, name: str, token_type: Algorithm, value: str):
        super().__init__(connection)
        self._name = name
        self._type = token_type
        self._value = value

    def if_not_exists(self) -> "DefineToken":
        self._if_not_exists = True
        return self

    def validate(self) -> List[str]:
        if not isinstance(self._type, Algorithm):
            return ["Invalid token type, you should use one of Algorithm enumerations"]
        return [OK]

    def _clean_str(self):
        return f'DEFINE TOKEN{self._exists()} {self._name} ON DATABASE \nTYPE {self._type.name} \n' \
               f'VALUE "{self._value}"{self._comment()}'


class DefineTable(Define, CanUsePermissions):
    """
    Represents DEFINE TABLE statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/table

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    
    DEFINE TABLE [ OVERWRITE | IF NOT EXISTS ] @name
    [ DROP ]
    [ SCHEMAFULL | SCHEMALESS ]
    [ TYPE [ ANY | NORMAL | RELATION [ IN | FROM ] @table [ OUT | TO ] @table ] ]
    [ AS SELECT @projections
        FROM @tables
        [ WHERE @condition ]
        [ GROUP [ BY ] @groups ]
    ]
    [CHANGEFEED @duration [INCLUDE ORIGINAL] ]
    [ PERMISSIONS [ NONE | FULL
        | FOR select @expression
        | FOR create @expression
        | FOR update @expression
        | FOR delete @expression
    ] ]
    [ COMMENT @string ]
    """

    def __init__(self, connection: Connection, name: str):
        super().__init__(connection)
        self._name = name
        self._drop = False
        self._less = False
        self._full = False
        self._alias = None
        self._changefeed = None
        self._type = None

    def if_not_exists(self) -> "DefineTable":
        """
        Represents IF NOT EXISTS statement
        """
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineTable":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

    def drop(self) -> "DefineTable":
        """
        Represents DROP statement
        """
        self._drop = True
        return self

    def schemafull(self) -> "DefineTable":
        """
        Represents SCHEMAFULL statement
        """
        self._full = True
        self._less = False
        return self

    def schemaless(self) -> "DefineTable":
        """
        Represents SCHEMALESS statement
        """
        self._full = False
        self._less = True
        return self

    def changefeed(self, duration: str, include_original: bool = False) -> "DefineTable":
        """
        Represents CHANGEFEED statement

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/table#using-changefeed-clause

        :param duration: valid string representation for duration, for example, "1s"
        :param include_original: if True, then add INCLUDE ORIGINAL statement
        """
        self._changefeed = (duration, include_original)
        return self

    def alias(self, select: Union[str, Statement]) -> "DefineTable":
        """
        Represents AS statements, Select statement or raw string expected
        :param select: Select statement or string
        """
        if isinstance(select, Statement):
            self._alias = select._clean_str()
        else:
            self._alias = select
        return self

    def type_any(self) -> "DefineTable":
        """
        Represents TYPE ANY statement
        """
        self._type = "ANY"
        return self

    def type_normal(self) -> "DefineTable":
        """
        Represents TYPE NORMAL statement
        """
        self._type = "NORMAL"
        return self

    def type_relation(self, from_to: Optional[Tuple] = None, use_from_to: bool = True) -> "DefineTable":
        """
        Represents TYPE RELATE statement

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/table#table-with-specialized-type-clause-since-140

        Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

        :param from_to: optional pair of source and target table, both must be provided(not None)
        :param use_from_to: if True, FROM TO syntax will be used, if False, IN OUT syntax will be used
        """
        if from_to and from_to[0] and from_to[1]:
            if use_from_to:
                self._type = f"RELATION FROM {from_to[0]} TO {from_to[1]}"
            else:
                self._type = f"RELATION IN {from_to[0]} OUT {from_to[1]}"
        else:
            self._type = "RELATION"
        return self

    def validate(self) -> List[str]:
        durations = ('w', 'y', 'd', 'h', 'ms', 's', 'm')
        if self._changefeed:
            actual_duration, _ = self._changefeed
            if not any(actual_duration.endswith(letter) for letter in durations):
                return [f"Wrong duration {actual_duration}, allowed postfix are {durations}"]
        return [OK]

    def _clean_str(self):
        drop = "" if not self._drop else " DROP"
        schema = ""
        if self._less:
            schema = " SCHEMALESS"
        elif self._full:
            schema = " SCHEMAFULL"
        alias = "" if not self._alias else f" AS\n {self._alias}\n"
        feed = ""
        if self._changefeed:
            feed = f" CHANGEFEED {self._changefeed[0]}"
            if self._changefeed[1]:
                feed = f"{feed} INCLUDE ORIGINAL"
        type_ = "" if not self._type else f" TYPE {self._type}"
        return f'DEFINE TABLE{self._exists()} {self._name}{drop}{schema}{type_}{alias}{feed}{self._comment()}'


class DefineField(Define, CanUsePermissions):
    """
    Represents DEFINE FIELD statement

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/field

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

    DEFINE FIELD [ OVERWRITE | IF NOT EXISTS ] @name ON [ TABLE ] @table
    [ [ FLEXIBLE ] TYPE @type ]
    [ DEFAULT @expression ]
  [ READONLY ]
    [ VALUE @expression ]
    [ ASSERT @expression ]
    [ PERMISSIONS [ NONE | FULL
        | FOR select @expression
        | FOR create @expression
        | FOR update @expression
        | FOR delete @expression
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

    def default(self, value: str) -> "DefineField":
        """
        Represents DEFAULT statement
        """
        self._default = value
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

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        type_ = "" if not self._type else f" TYPE {self._type}"
        if type_ and self._flex:
            type_ = f" FLEXIBLE{type_}"
        default = "" if not self._default else f" DEFAULT {self._default}"
        ro = "" if not self._readonly else " READONLY"
        value = "" if not self._value else f" VALUE {self._value}"
        asserts = "" if not self._assert else f" ASSERT {self._assert}"
        return f'DEFINE FIELD{self._exists()} {self._field_name} ' \
               f'ON TABLE {self._table_name}{type_}{default}{ro}{value}{asserts}{self._comment()}'
