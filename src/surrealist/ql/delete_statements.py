from surrealist.ql.common_statements import CanUseReturn
from surrealist.ql.statement import FinishedStatement, Statement


class Where(FinishedStatement, CanUseReturn):
    def __init__(self, statement: Statement, predicate: str):
        super().__init__(statement)
        self._predicate = predicate

    def _clean_str(self):
        return f"{self._statement._clean_str()} WHERE {self._predicate}"


class DeleteUseWhere(CanUseReturn):
    def where(self, predicate: str) -> Where:
        return Where(self, predicate)
