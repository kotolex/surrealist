import json
from typing import Dict, Optional

from surrealist.ql.statements.common_statements import CanUseReturn
from surrealist.ql.statements.statement import FinishedStatement, Statement


class Set(FinishedStatement, CanUseReturn):
    def __init__(self, statement: Statement, result: Optional[str] = None, **kwargs):
        super().__init__(statement)
        self._kw = kwargs
        self._result = result

    def _clean_str(self):
        if not self._kw and not self._result:
            return self._statement._clean_str()
        if not self._result:
            args = ", ".join(f"{k} = {json.dumps(v)}" for k, v in self._kw.items())
        else:
            args = self._result
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

    def set(self, result: Optional[str] = None, **kwargs) -> Set:
        return Set(self, result, **kwargs)
