import json
from typing import Dict, List, Optional

from surrealist.ql.statements.common_statements import CanUseWhere
from surrealist.ql.statements.statement import FinishedStatement, Statement


class Set(FinishedStatement, CanUseWhere):
    def __init__(self, statement: Statement, result:Optional[str] = None, **kwargs):
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


class Patch(FinishedStatement, CanUseWhere):
    def __init__(self, statement: Statement, operations: List[Dict]):
        super().__init__(statement)
        self._value = operations

    def _clean_str(self):
        return f"{self._statement._clean_str()} PATCH {json.dumps(self._value)}"


class Merge(FinishedStatement, CanUseWhere):
    def __init__(self, statement: Statement, value: Dict):
        super().__init__(statement)
        self._value = value

    def _clean_str(self):
        return f"{self._statement._clean_str()} MERGE {json.dumps(self._value)}"


class Content(FinishedStatement, CanUseWhere):
    def __init__(self, statement: Statement, value: Dict):
        super().__init__(statement)
        self._value = value

    def _clean_str(self):
        return f"{self._statement._clean_str()} CONTENT {json.dumps(self._value)}"


class UpdateUseMethods(CanUseWhere):
    def set(self, result:Optional[str]=None, **kwargs) -> Set:
        return Set(self, result, **kwargs)

    def content(self, value: Dict) -> Content:
        return Content(self, value)

    def merge(self, value: Dict) -> Merge:
        return Merge(self, value)

    def patch(self, operations: List[Dict]) -> Patch:
        return Patch(self, operations)
