from abc import ABC, abstractmethod
from typing import List

from surrealist import Connection, SurrealResult
from surrealist.utils import OK


class Statement(ABC):
    def __init__(self, connection: Connection):
        self._connection = connection

    def _validate(self):
        return OK

    @abstractmethod
    def validate(self) -> List[str]:
        pass

    def is_valid(self) -> bool:
        return all(mess == OK for mess in self.validate())

    @abstractmethod
    def _clean_str(self):
        pass

    def to_str(self):
        return f"{self._clean_str()};"

    def run(self) -> SurrealResult:
        return self._connection.query(self.to_str())

    def __str__(self):
        return self.to_str()


class FinishedStatement(Statement):
    def __init__(self, statement: Statement):
        self._connection = statement._connection
        super().__init__(self._connection)
        self._statement = statement

    def validate(self) -> List[str]:
        result = self._statement.validate()
        result.append(self._validate())
        return [e for e in result if e != OK]

    def _clean_str(self):
        return self._statement._clean_str()
