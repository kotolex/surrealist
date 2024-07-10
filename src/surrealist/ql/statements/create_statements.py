import json
from typing import Dict, Optional

from surrealist.ql.statements.common_statements import CanUseReturn
from surrealist.ql.statements.statement import FinishedStatement, Statement
from surrealist.ql.statements.utils import combine


class Set(FinishedStatement, CanUseReturn):
    """
    Represents a SET clause for CREATE statements.
    """
    def __init__(self, statement: Statement, result: Optional[str] = None, **kwargs):
        super().__init__(statement)
        self._kw = kwargs
        self._result = result

    def _clean_str(self):
        if not self._kw and not self._result:
            return self._statement._clean_str()
        args = combine(self._result, self._kw)
        return f"{self._statement._clean_str()} SET {args}"


class Content(FinishedStatement, CanUseReturn):
    """
    Represents a CONTENT clause for CREATE statements.
    """
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
