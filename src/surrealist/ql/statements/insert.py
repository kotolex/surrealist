import json
from typing import List, Dict

from surrealist.connections import Connection
from surrealist.ql.statements.insert_statements import InsertUseDuplicate
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK

"""

"""


class Insert(Statement, InsertUseDuplicate):
    """
    Represents INSERT INTO statement, it should be able to use any statements from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/insert

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_insert_examples.py

    INSERT [ IGNORE ] INTO @what
    [ @value
      | (@fields) VALUES (@values)
        [ ON DUPLICATE KEY UPDATE @field = @value ... ]
    ];
    """

    def __init__(self, connection: Connection, table_name: str, *args):
        """
        Init for Insert

        Notice: args can contain only one element if it is one of (List, Dict, Select) OR multiple tuples of the same
        length, representing names and values for insert

        Refer to: https://github.com/kotolex/surrealist/blob/master/examples/ql_insert_examples.py

        :param connection: underlying connection
        :param table_name: name of the table to operate on
        :param args: arguments
        """
        if not args:
            raise ValueError("Insert expects one or more arguments to insert")
        super().__init__(connection)
        self._table_name = table_name
        self._args = args

    def validate(self) -> List[str]:
        """
        Returns error if arguments are not appropriate for INSERT
        :return: list with OK or errors
        """
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
