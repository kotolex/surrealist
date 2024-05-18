from surrealist.ql.statements.statement import FinishedStatement, Statement


class OnDuplicate(FinishedStatement):
    """
    Represents ON DUPLICATE part of the INSERT statement

    [ ON DUPLICATE KEY UPDATE @field = @value ... ]
    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/insert
    """
    def __init__(self, statement: Statement, action: str):
        super().__init__(statement)
        self._action = action

    def _clean_str(self):
        return f"{self._statement._clean_str()} ON DUPLICATE KEY UPDATE {self._action}"


class InsertUseDuplicate:
    def on_duplicate(self, action: str) -> OnDuplicate:
        """
        Add ON DUPLICATE clause to final statement

        ON DUPLICATE KEY UPDATE @field = @value ...

        :param action: raw representation of action
        :return: OnDuplicate object
        """
        return OnDuplicate(self, action)
