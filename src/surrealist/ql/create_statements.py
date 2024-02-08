import json
from typing import Dict

from surrealist.ql.statement import FinishedStatement, Statement
from surrealist.utils import OK


class Parallel(FinishedStatement):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} PARALLEL"


class CreateUseParallel:
    def parallel(self) -> Parallel:
        return Parallel(self)


class Timeout(FinishedStatement, CreateUseParallel):
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


class CreateUseTimeout(CreateUseParallel):
    def timeout(self, duration: str) -> Timeout:
        return Timeout(self, duration)


class ReturnNone(FinishedStatement, CreateUseTimeout):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} RETURN NONE"


class ReturnBefore(FinishedStatement, CreateUseTimeout):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} RETURN BEFORE"


class ReturnAfter(FinishedStatement, CreateUseTimeout):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} RETURN AFTER"


class ReturnDiff(FinishedStatement, CreateUseTimeout):
    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} RETURN DIFF"


class Return(FinishedStatement, CreateUseTimeout):
    def __init__(self, statement: Statement, *args: str):
        super().__init__(statement)
        self._args = args

    def _clean_str(self):
        if not self._args:
            return self._statement._clean_str()
        args = ", ".join(self._args)
        return f"{self._statement._clean_str()} RETURN {args}"


class CreateUseReturn(CreateUseTimeout):
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


class Set(FinishedStatement, CreateUseReturn):
    def __init__(self, statement: Statement, **kwargs):
        super().__init__(statement)
        self._kw = kwargs

    def _clean_str(self):
        if not self._kw:
            return self._statement._clean_str()
        args = ", ".join(f"{k} = {json.dumps(v)}" for k, v in self._kw.items())
        return f"{self._statement._clean_str()} SET {args}"


class Content(FinishedStatement, CreateUseReturn):
    def __init__(self, statement: Statement, content: Dict):
        super().__init__(statement)
        self._content = content

    def _clean_str(self):
        args = json.dumps(self._content)
        return f"{self._statement._clean_str()} CONTENT {args}"


class CreateUseSetContent(CreateUseReturn):
    def content(self, content: Dict) -> Content:
        return Content(self, content)

    def set(self, **kwargs) -> Set:
        return Set(self, **kwargs)
