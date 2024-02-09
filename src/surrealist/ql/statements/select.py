from typing import Optional, Tuple, Union, List

from surrealist import Connection
from surrealist.utils import OK
from .select_statements import SelectUseIndex
from .statement import Statement


class Select(Statement, SelectUseIndex):
    def __init__(self, connection: Connection, table_name: Union[str, "Select"], *args,
                 alias: Optional[Tuple[str, Union[str, Statement]]] = None,
                 value: Optional[str] = None):
        super().__init__(connection)
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
            self._what = f"{what}{self._as[1]} AS {self._as[0]}"
        if self._value:
            self._what = f"VALUE {self._value}"

    def by_id(self, record_id: str) -> "Select":
        self._from = f"{self._table_name}:{record_id}"
        return self

    def only(self) -> "Select":
        self._only = True
        return self

    def omit(self, *fields: Tuple[str]) -> "Select":
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
