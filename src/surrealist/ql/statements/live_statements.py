from surrealist.result import SurrealResult
from surrealist.ql.statements.statement import FinishedStatement, Statement


class Fetch(FinishedStatement):
    def __init__(self, statement: Statement, *args: str):
        super().__init__(statement)
        self._args = args

    def _clean_str(self):
        if not self._args:
            return self._statement._clean_str()
        what = ", ".join(self._args)
        return f"{self._statement._clean_str()} FETCH {what}"

    def run(self) -> SurrealResult:
        return self._statement._drill(self.to_str())


class LiveUseFetch:
    def fetch(self, *args: str) -> Fetch:
        """
        Add fetch clause to final statement.
        NOTE: Support for FETCH is not yet available on SurrealDb side!!!

        :param args: arguments to fetch
        :return: Fetch object
        """
        return Fetch(self, *args)


class Where(FinishedStatement, LiveUseFetch):
    """
    Represents WHERE part of the LIVE statement

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/live#filter-the-live-query
    """
    def __init__(self, statement: Statement, predicate: str):
        super().__init__(statement)
        self._predicate = predicate

    def run(self) -> SurrealResult:
        return self._statement._drill(self.to_str())

    def _drill(self, query) -> SurrealResult:
        return self._statement._drill(query)

    def _clean_str(self):
        return f"{self._statement._clean_str()} WHERE {self._predicate}"


class LiveUseWhere(LiveUseFetch):
    def where(self, predicate: str) -> Where:
        """
        Add WHERE clause to filter data

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/live#filter-the-live-query

        :param predicate: condition to filter data
        :return: Where object
        """
        return Where(self, predicate)
