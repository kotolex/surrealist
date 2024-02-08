import json
from typing import Dict

from surrealist.ql.common_statements import CanUseReturn
from surrealist.ql.statement import FinishedStatement, Statement


class Set(FinishedStatement, CanUseReturn):
    def __init__(self, statement: Statement, **kwargs):
        super().__init__(statement)
        self._kw = kwargs

    def _clean_str(self):
        if not self._kw:
            return self._statement._clean_str()
        args = ", ".join(f"{k} = {json.dumps(v)}" for k, v in self._kw.items())
        return f"{self._statement._clean_str()} SET {args}"


class Content(FinishedStatement, CanUseReturn):
    def __init__(self, statement: Statement, content: Dict):
        super().__init__(statement)
        self._content = content

    def _clean_str(self):
        args = json.dumps(self._content)
        return f"{self._statement._clean_str()} CONTENT {args}"


class CreateUseSetContent(CanUseReturn):
    def content(self, content: Dict) -> Content:
        return Content(self, content)

    def set(self, **kwargs) -> Set:
        return Set(self, **kwargs)
