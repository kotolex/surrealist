from surrealist.ql.statements.statement import FinishedStatement, Statement


class OnDuplicate(FinishedStatement):
    def __init__(self, statement: Statement, action: str):
        super().__init__(statement)
        self._action = action

    def _clean_str(self):
        return f"{self._statement._clean_str()} ON DUPLICATE KEY UPDATE {self._action}"


class InsertUseDuplicate:
    def on_duplicate(self, action: str) -> OnDuplicate:
        return OnDuplicate(self, action)
