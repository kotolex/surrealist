from typing import Optional, Union

from surrealist.ql.statements.simple_statements import Where
from surrealist.ql.statements.statement import FinishedStatement, Statement


class PermissionsNone(FinishedStatement):
    def _clean_str(self):
        return f"{self._statement._clean_str()} PERMISSIONS NONE"


class PermissionsFull(FinishedStatement):
    def _clean_str(self):
        return f"{self._statement._clean_str()} PERMISSIONS FULL"


class PermissionsFor(FinishedStatement):
    def __init__(self, statement: Statement, select: Optional[Union[str, Where]] = None,
                 create: Optional[Union[str, Where]] = None,
                 update: Optional[Union[str, Where]] = None,
                 delete: Optional[Union[str, Where]] = None):
        super().__init__(statement)
        a_dict = {"select": select, "create": create, "update": update, "delete": delete}
        self._kw = {}
        for key, value in a_dict.items():
            if not value:
                continue
            if value not in self._kw:
                self._kw[value] = key
            else:
                self._kw[value] = f"{self._kw[value]}, {key}"
        self._kw = {v: k for k, v in self._kw.items()}

    def _clean_str(self):
        result = "\n FOR ".join(f"{k} {v}" for k, v in self._kw.items())
        return f"{self._statement._clean_str()} \nPERMISSIONS \n FOR {result}"


class CanUsePermissions:
    def permissions_none(self) -> PermissionsNone:
        """
        Represents PERMISSIONS NONE statement
        """
        return PermissionsNone(self)

    def permissions_full(self) -> PermissionsFull:
        """
        Represents PERMISSIONS FULL statement
        """
        return PermissionsFull(self)

    def permissions_for(self, **kwargs) -> PermissionsFor:
        """
        Represents PERMISSIONS FOR statement
        """
        return PermissionsFor(self, **kwargs)
