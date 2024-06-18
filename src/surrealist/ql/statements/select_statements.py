from typing import Tuple

from surrealist.ql.statements.statement import Statement, FinishedStatement, IterableStatement
from surrealist.utils import OK


class Explain(FinishedStatement):
    """ Explain is not a child of IterableStatement because it has always only one result"""

    def __init__(self, statement: Statement, full: bool = False):
        super().__init__(statement)
        self._full = full

    def _clean_str(self):
        final = " FULL" if self._full else ""
        return f"{self._statement._clean_str()} EXPLAIN{final}"


class SelectUseExplain:

    def explain(self) -> Explain:
        return Explain(self, full=False)

    def explain_full(self) -> Explain:
        return Explain(self, full=True)


class Tempfiles(IterableStatement, SelectUseExplain):
    def _clean_str(self):
        return f"{self._statement._clean_str()} TEMPFILES"


class SelectUseTempfiles(SelectUseExplain):

    def tempfiles(self) -> Tempfiles:
        """
        Include TEMPFILES statement for the query
        When processing a large result set with many records, it is possible to use the TEMPFILES clause to specify that
        the statement should be processed in temporary files rather than memory. This significantly reduces memory
        usage, though it will also result in slower performance

        Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/select#the-tempfiles-clause
        """
        return Tempfiles(self)


class Parallel(IterableStatement):

    def _clean_str(self):
        return f"{self._statement._clean_str()} PARALLEL"


class SelectUseParallel(SelectUseTempfiles):
    def parallel(self) -> Parallel:
        return Parallel(self)


class Timeout(IterableStatement, SelectUseParallel):
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


class SelectUseTimeout(SelectUseParallel):
    def timeout(self, duration: str) -> Timeout:
        return Timeout(self, duration)


class Fetch(IterableStatement, SelectUseTimeout):
    def __init__(self, statement: Statement, *args: str):
        super().__init__(statement)
        self._args = args

    def _clean_str(self):
        add = "" if not self._args else ", ".join(self._args)
        if not add:
            return self._statement
        return f"{self._statement._clean_str()} FETCH {add}"


class SelectUseFetch(SelectUseTimeout):
    def fetch(self, *args) -> Fetch:
        return Fetch(self, *args)


class Start(IterableStatement, SelectUseFetch):
    def __init__(self, statement: Statement, offset: int):
        super().__init__(statement)
        self._offset = offset

    def _validate(self):
        if self._offset < 0:
            return "Offset should not be negative number"
        return OK

    def _clean_str(self):
        return f"{self._statement._clean_str()} START {self._offset}"


class SelectUseStart(SelectUseFetch):
    def start_at(self, offset: int) -> Start:
        return Start(self, offset)


class Limit(IterableStatement, SelectUseStart):
    def __init__(self, statement: Statement, limit: int):
        super().__init__(statement)
        self._limit = limit

    def _validate(self):
        if self._limit < 1:
            return "Limit should not be less than 1"
        return OK

    def _clean_str(self):
        return f"{self._statement._clean_str()} LIMIT {self._limit}"


class SelectUseLimit(SelectUseStart):
    def limit(self, limit: int) -> Limit:
        return Limit(self, limit)


class OrderByRand(IterableStatement, SelectUseLimit):
    def __init__(self, statement: Statement):
        super().__init__(statement)
        self._statement = statement

    def _clean_str(self):
        return f"{self._statement._clean_str()} ORDER BY RAND()"


class OrderBy(IterableStatement, SelectUseLimit):
    def __init__(self, statement: Statement, *args: str):
        super().__init__(statement)
        self._statement = statement
        self._args = args

    def _clean_str(self):
        if not self._args:
            return self._statement
        what = ", ".join(self._args)
        return f"{self._statement._clean_str()} ORDER BY {what}"


class SelectUseOrder(SelectUseLimit):
    def order_by(self, *args: str) -> OrderBy:
        return OrderBy(self, *args)

    def order_by_rand(self) -> OrderByRand:
        return OrderByRand(self)


class GroupBy(IterableStatement, SelectUseOrder):
    def __init__(self, statement: Statement, *args):
        super().__init__(statement)
        self._statement = statement
        self._args = args

    def _clean_str(self):
        if not self._args:
            return self._statement
        what = ", ".join(self._args)
        return f"{self._statement._clean_str()} GROUP BY {what}"


class GroupAll(IterableStatement, SelectUseOrder):
    def __init__(self, statement: Statement):
        super().__init__(statement)
        self._statement = statement

    def _clean_str(self):
        return f"{self._statement._clean_str()} GROUP ALL"


class SelectUseGroup(SelectUseOrder):
    def group_by(self, *args: str) -> GroupBy:
        return GroupBy(self, *args)

    def group_all(self) -> GroupAll:
        return GroupAll(self)


class Split(IterableStatement, SelectUseGroup):
    def __init__(self, statement: Statement, field: str):
        super().__init__(statement)
        self._statement = statement
        self._field = field

    def _clean_str(self):
        return f"{self._statement._clean_str()} SPLIT {self._field}"


class SelectUseSplit(SelectUseGroup):
    def split(self, field: str) -> Split:
        return Split(self, field)


class Or(IterableStatement, SelectUseSplit):
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


class And(IterableStatement, SelectUseSplit):
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


class Where(IterableStatement, SelectUseSplit):
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


class SelectUseWhere(SelectUseSplit):
    def where(self, predicate: str) -> Where:
        return Where(self, predicate)


class WithIndex(IterableStatement, SelectUseWhere):
    def __init__(self, statement: Statement, *index_names: str):
        super().__init__(statement)
        self._statement = statement
        self._index = index_names

    def _clean_str(self):
        if not self._index:
            return self._statement
        indexes = ", ".join(self._index)
        return f"{self._statement._clean_str()} WITH INDEX {indexes}"


class WithNoIndex(IterableStatement, SelectUseWhere):
    def __init__(self, statement: Statement):
        super().__init__(statement)
        self._statement = statement

    def _clean_str(self):
        return f"{self._statement._clean_str()} WITH NO INDEX"


class SelectUseIndex(SelectUseWhere):
    def with_index(self, *args: Tuple[str]) -> WithIndex:
        return WithIndex(self, *args)

    def with_no_index(self) -> WithNoIndex:
        return WithNoIndex(self)
