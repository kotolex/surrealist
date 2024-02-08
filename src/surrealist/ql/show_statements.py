from datetime import datetime

from surrealist.ql.statement import FinishedStatement, Statement
from surrealist.utils import OK, DATE_FORMAT, DATE_FORMAT_NS


class Limit(FinishedStatement):
    def __init__(self, statement: Statement, limit: int):
        super().__init__(statement)
        self._limit = limit

    def _validate(self):
        if self._limit < 1:
            return "Limit should not be less than 1"
        return OK

    def _clean_str(self):
        return f"{self._statement._clean_str()} LIMIT {self._limit}"


class ShowUseLimit:
    def limit(self, limit: int) -> Limit:
        return Limit(self, limit)


class Since(FinishedStatement, ShowUseLimit):
    def __init__(self, statement: Statement, timestamp: str):
        super().__init__(statement)
        self._timestamp = timestamp

    def _validate(self):
        try:
            format_ = DATE_FORMAT if "." not in self._timestamp else DATE_FORMAT_NS
            datetime.strptime(self._timestamp, format_)
        except ValueError:
            return "Timestamp in wrong format, you need iso-date like 2024-01-01T10:10:10.000001Z"
        return OK

    def _clean_str(self):
        return f'{self._statement._clean_str()} SINCE "{self._timestamp}"'


class ShowUseSince(ShowUseLimit):
    def since(self, timestamp: str) -> Since:
        return Since(self, timestamp)
