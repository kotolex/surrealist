from typing import List

from surrealist.connections.connection import Connection
from surrealist.utils import OK
from .define import Define

"""
DEFINE USER [ IF NOT EXISTS ] @name
    ON [ ROOT | NAMESPACE | DATABASE ]
    [ PASSWORD @pass | PASSHASH @hash ]
    [ ROLES @roles ]
    [ DURATION [ FOR TOKEN @duration [ , ] ] [ FOR SESSION @duration ] ]
  [ COMMENT @string ]
"""


class DefineUser(Define):
    """
    Represents DEFINE USER ON DATABASE statement, a default role is VIEWER

    Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/user

    Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

    """

    def __init__(self, connection: Connection, user_name: str):
        super().__init__(connection)
        self._name = user_name
        self._role = None
        self._pass = None
        self._hash = None
        self._durations = {"token": None, "session": None}

    def if_not_exists(self) -> "DefineUser":
        self._if_not_exists = True
        return self

    def validate(self) -> List[str]:
        for duration in self._durations.values():
            if not duration:
                continue
            if not any(duration.endswith(letter) for letter in ('d', 'h', 's', 'm')):
                return [f"Wrong duration {duration}, allowed postfix are ('d', 'h', 's', 'm')"]
            if ' ' in duration:
                return ["Wrong duration format, should be like 5h"]
        return [OK]

    def password(self, value: str) -> "DefineUser":
        self._pass = value
        self._hash = None
        return self

    def passhash(self, value: str) -> "DefineUser":
        self._pass = None
        self._hash = value
        return self

    def role_owner(self) -> "DefineUser":
        """
        Represents role OWNER

        """
        self._role = "OWNER"
        return self

    def role_editor(self) -> "DefineUser":
        """
        Represents role EDITOR

        """
        self._role = "EDITOR"
        return self

    def role_viewer(self) -> "DefineUser":
        """
        Represents role VIEWER

        """
        self._role = "VIEWER"
        return self

    def duration_token(self, duration: str) -> "DefineUser":
        self._durations["token"] = duration
        return self

    def duration_session(self, duration: str) -> "DefineUser":
        self._durations["session"] = duration
        return self

    def _clean_str(self):
        a_list = []
        if self._durations["token"]:
            a_list.append(f"DURATION FOR TOKEN {self._durations['token']}")
        if self._durations["session"]:
            a_list.append(f"DURATION FOR SESSION {self._durations['session']}")
        durations = ", ".join(a_list)
        if durations:
            durations = f" {durations}"
        pass_ = ""
        if self._pass:
            pass_ = f" PASSWORD '{self._pass}'"
        if self._hash:
            pass_ = f" PASSHASH '{self._hash}'"
        roles = "" if not self._role else f" ROLES {self._role}"
        return f"DEFINE USER{self._exists()} {self._name} ON DATABASE{pass_}{roles}{durations}{self._comment()}"
