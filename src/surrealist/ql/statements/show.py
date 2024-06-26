import json
from datetime import datetime, timezone
from typing import List, Optional, Union

from surrealist.connections import Connection
from surrealist.utils import OK, DATE_FORMAT, DATE_FORMAT_NS, to_surreal_datetime_str
from .statement import Statement

"""
SHOW CHANGES FOR TABLE @tableName SINCE "@timestamp" [LIMIT @number]
"""


class Show(Statement):
    """
    Represents SHOW CHANGES statement, it should be able to use any statements from documentation

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/show

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/ql_show_examples.py
    """

    def __init__(self, connection: Connection, table_name: str, since: Optional[str] = None):
        super().__init__(connection)
        self._table_name = table_name
        self._since = since
        self._limit = None

    def validate(self) -> List[str]:
        result = []
        if self._since and isinstance(self._since, str):
            try:
                format_ = DATE_FORMAT if "." not in self._since else DATE_FORMAT_NS
                datetime.strptime(self._since, format_)
            except ValueError:
                result.append("Timestamp in the wrong format, you need iso-date like 2024-01-01T10:10:10.000001Z")
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

    def since(self, timestamp: str) -> "Show":
        """
        Init timestamp since is to show updates

        Refer to: https://surrealdb.com/docs/surrealdb/surrealql/statements/show#basic-usage

        :param timestamp: surreal timestamp
        :return: Show object
        """
        self._since = timestamp
        return self

    def _clean_str(self):
        if not self._since:
            self._since = to_surreal_datetime_str(datetime.now(timezone.utc))  # default value
        limit = f" LIMIT {self._limit}" if self._limit else ""
        return f'SHOW CHANGES FOR TABLE {self._table_name} SINCE d{json.dumps(self._since)}{limit}'
