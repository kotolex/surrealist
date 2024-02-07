from typing import Tuple

from surrealist.ql.statement import Statement, FinishedStatement
from surrealist.utils import OK


class Explain(FinishedStatement):
    def __init__(self, statement: Statement, full: bool = False):
        super().__init__(statement)
        self._full = full

    def _clean_str(self):
        final = " FULL" if self._full else ""
        return f"{self._statement._clean_str()} EXPLAIN{final}"


class SelectFirstLevel:

    def explain(self) -> Explain:
        return Explain(self, full=False)

    def explain_full(self) -> Explain:
        return Explain(self, full=True)


class Parallel(FinishedStatement, SelectFirstLevel):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} PARALLEL"


class SelectSecondLevel(SelectFirstLevel):
    def parallel(self) -> Parallel:
        return Parallel(self)


class Timeout(FinishedStatement, SelectSecondLevel):
    def __init__(self, statement: Statement, duration: str):
        super().__init__(statement)
        self._duration = duration

    def _validate(self):
        if not any(self._duration.endswith(letter) for letter in ('ms', 's', 'm')):
            return f"Wrong duration {self._duration}, allowed postfix are (ms, s, m)"
        if ' ' in self._duration:
            return "Wrong duration format, should be like 5s"
        return OK

    def _clean_str(self):
        return f"{self._statement._clean_str()} TIMEOUT {self._duration}"


class SelectThirdLevel(SelectSecondLevel):
    def timeout(self, duration: str) -> Timeout:
        return Timeout(self, duration)


class Fetch(FinishedStatement, SelectThirdLevel):
    def __init__(self, statement: Statement, *args: str):
        super().__init__(statement)
        self._args = args

    def _clean_str(self):
        add = "" if not self._args else ", ".join(self._args)
        if not add:
            return self._statement
        return f"{self._statement._clean_str()} FETCH {add}"


class SelectFourthLevel(SelectThirdLevel):
    def fetch(self, *args) -> Fetch:
        return Fetch(self, *args)


class Start(FinishedStatement, SelectFourthLevel):
    def __init__(self, statement: Statement, offset: int):
        super().__init__(statement)
        self._offset = offset

    def _validate(self):
        if self._offset < 0:
            return "Offset should not be negative number"
        return OK

    def _clean_str(self):
        return f"{self._statement._clean_str()} START {self._offset}"


class SelectFifthLevel(SelectFourthLevel):
    def start_at(self, offset: int) -> Start:
        return Start(self, offset)


class Limit(FinishedStatement, SelectFifthLevel):
    def __init__(self, statement: Statement, limit: int):
        super().__init__(statement)
        self._limit = limit

    def _validate(self):
        if self._limit < 1:
            return "Limit should not be less than 1"
        return OK

    def _clean_str(self):
        return f"{self._statement._clean_str()} LIMIT {self._limit}"


class SelectSixthLevel(SelectFifthLevel):
    def limit(self, limit: int) -> Limit:
        return Limit(self, limit)


class OrderByRand(FinishedStatement, SelectSixthLevel):
    def __init__(self, statement: Statement):
        super().__init__(statement)
        self._statement = statement

    def _clean_str(self):
        return f"{self._statement._clean_str()} ORDER BY RAND()"


class OrderBy(FinishedStatement, SelectSixthLevel):
    def __init__(self, statement: Statement, *args: str):
        super().__init__(statement)
        self._statement = statement
        self._args = args

    def _clean_str(self):
        if not self._args:
            return self._statement
        what = ", ".join(self._args)
        return f"{self._statement._clean_str()} ORDER BY {what}"


class SelectSeventhLevel(SelectSixthLevel):
    def order_by(self, *args: str) -> OrderBy:
        return OrderBy(self, *args)

    def order_by_rand(self) -> OrderByRand:
        return OrderByRand(self)


class GroupBy(FinishedStatement, SelectSeventhLevel):
    def __init__(self, statement: Statement, *args):
        super().__init__(statement)
        self._statement = statement
        self._args = args

    def _clean_str(self):
        if not self._args:
            return self._statement
        what = ", ".join(self._args)
        return f"{self._statement._clean_str()} GROUP BY {what}"


class GroupAll(FinishedStatement, SelectSeventhLevel):
    def __init__(self, statement: Statement):
        super().__init__(statement)
        self._statement = statement

    def _clean_str(self):
        return f"{self._statement._clean_str()} GROUP ALL"


class SelectEightLevel(SelectSeventhLevel):
    def group_by(self, *args: str) -> GroupBy:
        return GroupBy(self, *args)

    def group_all(self) -> GroupAll:
        return GroupAll(self)


class Split(FinishedStatement, SelectEightLevel):
    def __init__(self, statement: Statement, field: str):
        super().__init__(statement)
        self._statement = statement
        self._field = field

    def _clean_str(self):
        return f"{self._statement._clean_str()} SPLIT {self._field}"


class SelectNinthLevel(SelectEightLevel):
    def split(self, field: str) -> Split:
        return Split(self, field)


class Or(FinishedStatement, SelectNinthLevel):
    def __init__(self, statement: Statement, predicate: str):
        super().__init__(statement)
        self._statement = statement
        self._predicate = predicate

    def AND(self, predicate):
        return And(self, predicate)

    def OR(self, predicate):
        return Or(self, predicate)

    def _clean_str(self):
        return f"{self._statement._clean_str()} OR {self._predicate}"


class And(FinishedStatement, SelectNinthLevel):
    def __init__(self, statement: Statement, predicate: str):
        super().__init__(statement)
        self._statement = statement
        self._predicate = predicate

    def AND(self, predicate):
        return And(self, predicate)

    def OR(self, predicate):
        return Or(self, predicate)

    def _clean_str(self):
        return f"{self._statement._clean_str()} AND {self._predicate}"


class Where(FinishedStatement, SelectNinthLevel):
    def __init__(self, statement: Statement, predicate: str):
        super().__init__(statement)
        self._statement = statement
        self._predicate = predicate

    def AND(self, predicate):
        return And(self, predicate)

    def OR(self, predicate):
        return Or(self, predicate)

    def _clean_str(self):
        return f"{self._statement._clean_str()} WHERE {self._predicate}"


class SelectTenthLevel(SelectNinthLevel):
    def where(self, predicate: str) -> Where:
        return Where(self, predicate)


class WithIndex(FinishedStatement, SelectTenthLevel):
    def __init__(self, statement: Statement, *index_names: str):
        super().__init__(statement)
        self._statement = statement
        self._index = index_names

    def _clean_str(self):
        if not self._index:
            return self._statement
        indexes = ", ".join(self._index)
        return f"{self._statement._clean_str()} WITH INDEX {indexes}"


class WithNoIndex(FinishedStatement, SelectTenthLevel):
    def __init__(self, statement: Statement):
        super().__init__(statement)
        self._statement = statement

    def _clean_str(self):
        return f"{self._statement._clean_str()} WITH NO INDEX"


class SelectEleventhLevel(SelectTenthLevel):
    def with_index(self, *args: Tuple[str]) -> WithIndex:
        return WithIndex(self, *args)

    def with_no_index(self) -> WithNoIndex:
        return WithNoIndex(self)
