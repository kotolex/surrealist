from surrealist.ql.statement import FinishedStatement, Statement
from surrealist.utils import OK


class Parallel(FinishedStatement):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} PARALLEL"


class CanUseParallel:
    def parallel(self) -> Parallel:
        return Parallel(self)


class Timeout(FinishedStatement, CanUseParallel):
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


class CanUseTimeout(CanUseParallel):
    def timeout(self, duration: str) -> Timeout:
        return Timeout(self, duration)


class ReturnNone(FinishedStatement, CanUseTimeout):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} RETURN NONE"


class ReturnBefore(FinishedStatement, CanUseTimeout):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} RETURN BEFORE"


class ReturnAfter(FinishedStatement, CanUseTimeout):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} RETURN AFTER"


class ReturnDiff(FinishedStatement, CanUseTimeout):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} RETURN DIFF"


class Return(FinishedStatement, CanUseTimeout):
    def __init__(self, statement: Statement, *args: str):
        super().__init__(statement)
        self._args = args

    def _clean_str(self):
        if not self._args:
            return self._statement._clean_str()
        args = ", ".join(self._args)
        return f"{self._statement._clean_str()} RETURN {args}"


class CanUseReturn(CanUseTimeout):
    def return_diff(self) -> ReturnDiff:
        return ReturnDiff(self)

    def return_none(self) -> ReturnNone:
        return ReturnNone(self)

    def return_before(self) -> ReturnBefore:
        return ReturnBefore(self)

    def return_after(self) -> ReturnAfter:
        return ReturnAfter(self)

    def returns(self, *args: str) -> Return:
        return Return(self, *args)


class Where(FinishedStatement, CanUseReturn):
    def __init__(self, statement: Statement, predicate: str):
        super().__init__(statement)
        self._predicate = predicate

    def _clean_str(self):
        return f"{self._statement._clean_str()} WHERE {self._predicate}"


class CanUseWhere(CanUseReturn):
    def where(self, predicate: str) -> Where:
        return Where(self, predicate)
