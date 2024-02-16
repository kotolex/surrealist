from abc import ABC, abstractmethod
from typing import List, Iterator

from surrealist.connections import Connection
from surrealist.result import SurrealResult
from surrealist.utils import OK


class Statement(ABC):
    """
    Parent for all statements(QL statements)
    """

    def __init__(self, connection: Connection):
        self._connection = connection

    def _validate(self) -> str:
        return OK

    @abstractmethod
    def validate(self) -> List[str]:
        """
        Should check all parts of the query ant returns list of OK or errors
        :return: list of strings
        """

    def is_valid(self) -> bool:
        """
        Checks is whole query is valid
        :return: True, f query is valid, False otherwise
        """
        return all(mess == OK for mess in self.validate())

    @abstractmethod
    def _clean_str(self) -> str:
        """
        Returns query without ";" to work with
        :return: query text
        """

    def to_str(self):
        """
        Returns the whole query with ";" at the end
        :return: full query text
        """
        return f"{self._clean_str()};"

    def _drill(self, query) -> SurrealResult:
        """
        This method for live queries only
        :param query: full query text
        :return: result of the query
        """

    def run(self) -> SurrealResult:
        """
        Runs the whole query and returns result from SurrealDB
        :return: result of the request
        """
        return self._connection.query(self.to_str())

    def __str__(self):
        return self.to_str()


class FinishedStatement(Statement):
    """
    Represents specific statement
    """

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


class IterableStatement(FinishedStatement):
    """
    Represents a statement which can use iterator to get results in an efficient, lazy way.
    Under the hood transform query to SELECT * FROM (initial_query) LIMIT {limit} START AT {current};
    """

    def iter(self, limit: int = 100) -> Iterator:
        """
        Creates and returns a generator object to iterate on big query results

        In documentation: https://github.com/kotolex/surrealist/tree/master?tab=readme-ov-file#iteration-on-select

        Example: https://github.com/kotolex/surrealist/tree/master/examples/surreal_ql/iterator.py

        :param limit: number of records in each iteration, it cannot be smaller than one
        :return: generator to use in for-statements or with the next method
        :raise ValueError: if limit less than one
        """
        if limit < 1:
            raise ValueError("The limit cannot be smaller than 1")
        current = 0
        while True:
            query = f"SELECT * FROM ({self._clean_str()}) LIMIT {limit} START AT {current};"
            res = self._connection.query(query)
            yield res
            if res.count() < limit:
                break
            current += limit
