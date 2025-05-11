from typing import List, Optional

from surrealist.connections import Connection
from surrealist.utils import OK, StrOrInt

from .statement import Statement


class Show(Statement):
    """
    Represents SHOW CHANGES statement, it should be able to use any statements from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/show

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_show_examples.py

    SHOW CHANGES FOR TABLE @tablename SINCE @timestamp | @versionstamp [ LIMIT @number ]

    """

    def __init__(self, connection: Connection, table_name: str, since: Optional[StrOrInt] = None):
        super().__init__(connection)
        self._table_name = table_name
        self._since = since
        self._limit = None

    def validate(self) -> List[str]:
        result = []
        if self._since and isinstance(self._since, str):
            if not self._since.startswith("d"):
                result.append("Timestamp in the wrong format, you need iso-date like d'2024-01-01T10:10:10.000001Z'")
        if self._limit and self._limit < 1:
            result.append("Limit should not be less than 1")
        return [OK] if not result else result

    def limit(self, limit: int) -> "Show":
        """
        Limits number of results to show (LIMIT statement)
        :param limit: number of results
        :return: Show object
        """
        self._limit = limit
        return self

    def since(self, datetime_or_versionstamp: StrOrInt) -> "Show":
        """
        Init timestamp or versionstamp since is to show updates,
        it should be a surreal timestamp like d'2024-01-01T10:10:10.000001Z' or an integer versionstamp

        Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/show#basic-usage

        :param datetime_or_versionstamp: surreal timestamp or versionstamp
        :return: Show object
        """
        self._since = datetime_or_versionstamp
        return self

    def _clean_str(self):
        if not self._since:
            self._since = 1 # default value, shows all changes
        limit = f" LIMIT {self._limit}" if self._limit else ""
        return f'SHOW CHANGES FOR TABLE {self._table_name} SINCE {self._since}{limit}'
