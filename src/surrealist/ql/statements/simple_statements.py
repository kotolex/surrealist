from typing import Optional

from surrealist.ql.statements.utils import combine


class Where:
    """
    Represents simple WHERE statement for DEFINE FIELD and DEFINE TABLE
    """
    def __init__(self, raw_string: Optional[str] = None, **kwargs):
        self._raw = raw_string
        self._kw = kwargs
        self._body = combine(self._raw, self._kw)
        self._body = f"WHERE {self._body}"

    def __str__(self):
        return self._body

    def __repr__(self):
        return f"Where({self._raw=}, {self._kw=})"

    def OR(self, raw_string: Optional[str] = None, **kwargs) -> "OrAnd":
        """
        Represents OR statement in WHERE
        """
        return OrAnd(self, True, raw_string, **kwargs)

    def AND(self, raw_string: Optional[str] = None, **kwargs) -> "OrAnd":
        """
        Represents AND statement in WHERE
        """
        return OrAnd(self, False, raw_string, **kwargs)


class OrAnd(Where):
    def __init__(self, parent: Where, use_or: bool = True, raw_string: Optional[str] = None, **kwargs):
        super().__init__(raw_string, **kwargs)
        self._or = use_or
        self._parent = parent
        self._cur_body = combine(raw_string, kwargs)
        self._type = "OR" if self._or else "AND"
        self._body = f"{parent} {self._type} {self._cur_body}"

    def __repr__(self):
        return f"{self._type}({self._raw=}, {self._kw=}, {self._parent:!r})"
