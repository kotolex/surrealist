import json
from typing import Dict, List, Optional

from surrealist.ql.statements.common_statements import CanUseWhere
from surrealist.ql.statements.statement import FinishedStatement, Statement
from surrealist.ql.statements.utils import combine


class Set(FinishedStatement, CanUseWhere):
    """
    Represents SET part of the UPDATE statement
    SET @field = @value ...

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/update
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


class Patch(FinishedStatement, CanUseWhere):
    """
    Represents PATCH part of the UPDATE statement
    PATCH @value

    About patch: https://jsonpatch.com

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/update
    """
    def __init__(self, statement: Statement, operations: List[Dict]):
        super().__init__(statement)
        self._value = operations

    def _clean_str(self):
        return f"{self._statement._clean_str()} PATCH {json.dumps(self._value)}"


class Merge(FinishedStatement, CanUseWhere):
    """
    Represents MERGE part of the UPDATE statement
    MERGE @value

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/update
    """
    def __init__(self, statement: Statement, value: Dict):
        super().__init__(statement)
        self._value = value

    def _clean_str(self):
        return f"{self._statement._clean_str()} MERGE {json.dumps(self._value)}"


class Content(FinishedStatement, CanUseWhere):
    """
    Represents CONTENT part of the UPDATE statement
    CONTENT @value

    Refer to:
    https://surrealdb.com/docs/surrealdb/surrealql/statements/update
    """
    def __init__(self, statement: Statement, value: Dict):
        super().__init__(statement)
        self._value = value

    def _clean_str(self):
        return f"{self._statement._clean_str()} CONTENT {json.dumps(self._value)}"


class UpdateUseMethods(CanUseWhere):
    def set(self, result: Optional[str] = None, **kwargs) -> Set:
        """
        Sets the values for UPDATE statement.
        If both result and kwargs are used - will combine it, for example
        set("title ='Title'", text = "Text") will be 'SET title="Title", text="Text"'

        :param result: raw string to use for a complicated statement
        :param kwargs: simple key-values to use in statement
        :return: Set object
        """
        return Set(self, result, **kwargs)

    def content(self, value: Dict) -> Content:
        """
        Updates the record with given dictionary
        :param value: dict of data to update
        :return: Content oobject
        """
        return Content(self, value)

    def merge(self, value: Dict) -> Merge:
        """
        Merge (add) data to record
        :param value: dict with data to merge
        :return: Merge object
        """
        return Merge(self, value)

    def patch(self, operations: List[Dict]) -> Patch:
        """
        Patch (replace) data in record

        About patch format : https://jsonpatch.com

        :param operations: json-patch operations to update
        :return: Patch
        """
        return Patch(self, operations)
