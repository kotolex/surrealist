from typing import List, Optional

from surrealist import Connection
from surrealist.ql.statement import Statement
from surrealist.utils import OK


class Remove(Statement):
    _variants = ("TABLE", "EVENT", "FIELD", "INDEX", "PARAM")

    def __init__(self, connection: Connection, table_name: str, type_: str = "TABLE", name: Optional[str] = None):
        if type_ not in Remove._variants:
            raise ValueError(f"Type should be one of {Remove._variants}")
        super().__init__(connection)
        self._table_name = table_name
        self._only = False
        self._type = type_
        self._name = name
        if type_ == "TABLE":
            self._name = table_name

    def validate(self) -> List[str]:
        return [OK]

    def _clean_str(self):
        if self._type in ("TABLE", "PARAM"):
            what = f"{self._type} {self._name}"
        else:
            what = f"{self._type} {self._name} ON TABLE {self._table_name}"
        return f"REMOVE {what}"
