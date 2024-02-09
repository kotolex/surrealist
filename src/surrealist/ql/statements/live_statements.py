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


class LiveUseFetch:
    def fetch(self, *args: str) -> Fetch:
        return Fetch(self, *args)


class Where(FinishedStatement, LiveUseFetch):
    def __init__(self, statement: Statement, predicate: str):
        super().__init__(statement)
        self._predicate = predicate

    def _clean_str(self):
        return f"{self._statement._clean_str()} WHERE {self._predicate}"


class LiveUseWhere(LiveUseFetch):
    def where(self, predicate: str) -> Where:
        return Where(self, predicate)
