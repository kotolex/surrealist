from typing import Optional, Tuple, Union, List

from surrealist.connections import Connection
from surrealist.utils import OK
from .select_statements import SelectUseIndex, SelectUseTempfiles
from .statement import Statement, IterableStatement

"""
SELECT [ VALUE ] @fields [ AS @alias ]
    [ OMIT @fields ...]
    FROM [ ONLY ] @targets
    [ WITH [ NOINDEX | INDEX @indexes ... ]]
    [ WHERE @conditions ]
    [ SPLIT [ AT ] @field ... ]
    [ GROUP [ BY ] @fields ... ]
    [ ORDER [ BY ]
        @fields [
            RAND()
            | COLLATE
            | NUMERIC
        ] [ ASC | DESC ] ...
    ]
    [ LIMIT [ BY ] @limit ]
    [ START [ AT ] @start ]
    [ FETCH @fields ... ]
    [ TIMEOUT @duration ]
    [ PARALLEL ]
    [TEMPFILES]
    [ EXPLAIN [ FULL ]]
;
"""


class Select(IterableStatement, SelectUseIndex, SelectUseTempfiles):
    """
    Represents SELECT statement, it should be able to use any statements from documentation.
    It can use iterator to traverse results

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/select

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_select_examples.py

    Iterator example: https://github.com/kotolex/surrealist/tree/master/examples/surreal_ql/iterator.py
    """

    def __init__(self, connection: Connection, table_name: Union[str, Statement], *args,
                 alias: Optional[List[Tuple[str, Union[str, Statement]]]] = None,
                 value: Optional[str] = None):
        self._connection = connection
        super().__init__(self)
        self._table_name = table_name if isinstance(table_name, str) else f"({table_name._clean_str()})"
        self._args = args
        self._what = ""
        self._from = self._table_name
        self._only = False
        self._value = value
        self._as = alias
        self._update_args()
        self._omit = None

    def _update_args(self):
        if self._args:
            self._what = ", ".join(self._args)
        if self._as:
            what = "" if not self._what else f"{self._what}, "
            result = []
            for alias, value in self._as:
                value = value if not isinstance(value, Statement) else f"({value._clean_str()})"
                result.append(f"{value} AS {alias}")
            final_str = ", ".join(result)
            self._what = f"{what}{final_str}"
        if self._value:
            self._what = f"VALUE {self._value}"

    def by_id(self, record_id: str) -> "Select":
        """
        Select table_name:record_id

        :param record_id: id of the record to select
        """
        self._from = f"{self._table_name}:{record_id}"
        return self

    def only(self) -> "Select":
        """
        Include ONLY statement for the query
        """
        self._only = True
        return self

    def omit(self, *fields: Tuple[str]) -> "Select":
        """
        Include OMIT statement for the query
        :param fields: fields to omit
        """
        self._omit = fields
        return self

    def validate(self) -> List[str]:
        if self._only and ":" not in self._from:
            return ["Expected a single result output when using the ONLY keyword"]
        return [OK]

    def _clean_str(self):
        only = "ONLY " if self._only else ""
        what = "*" if not self._what else self._what
        omit = ""
        if self._omit:
            omitted = ", ".join(self._omit)
            omit = f" OMIT {omitted}"
        return f"SELECT {what}{omit} FROM {only}{self._from}"

    def __repr__(self):
        return f"Select({self._table_name=}, {self._args=}, {self._as=}, {self._value=})"
