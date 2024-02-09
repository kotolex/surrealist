import json
from typing import List, Dict

from surrealist import Connection
from surrealist.ql.statements.insert_statements import InsertUseDuplicate
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK


class Insert(Statement, InsertUseDuplicate):

    def __init__(self, connection: Connection, table_name: str, *args):
        """Can use Statement"""
        if not args:
            raise ValueError("Insert expects one or more arguments to insert")
        super().__init__(connection)
        self._table_name = table_name
        self._args = args

    def validate(self) -> List[str]:
        args = self._args
        if len(args) > 1:
            key_len = len(args[0])
            for other in args[1:]:
                if len(other) != key_len:
                    return ["Length of names is not equal to one of the values tuple"]
        if len(args) == 1:
            if not isinstance(args[0], (List, Dict, Statement)):
                return ["If only one argument, it can be list, dict or Statement"]
        return [OK]

    def _clean_str(self):
        args = self._args
        if len(args) == 1:
            what = f"({args[0]._clean_str()})" if isinstance(args[0], Statement) else json.dumps(args[0])
        else:
            names = f"({', '.join(args[0])})"
            data = ", ".join(str(e) for e in args[1:])
            what = f"{names} VALUES {data}"
        return f"INSERT INTO {self._table_name} {what}"
