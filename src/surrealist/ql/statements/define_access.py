from typing import List, Optional, Union

from surrealist.connections.connection import Connection
from surrealist.enums import Algorithm
from surrealist.ql.statements.create import Create
from surrealist.ql.statements.select import Select
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK


class DefineAccessBearer(Statement):
    """
    A bearer access method allows generating bearer grants with an associated key that can be used to access SurrealDB
    as a specific system user or record user. Bearer grants allow other systems and software to authenticate
    with SurrealDB using a secure and unique credential that can be audited and revoked at any time.

    Refer to: https://surrealdb.com/docs/surrealql/statements/define/access/bearer

    DEFINE ACCESS [ OVERWRITE | IF NOT EXISTS ] @name
    ON [ NAMESPACE | DATABASE ]
    TYPE BEARER FOR [ USER | RECORD ]
    [ AUTHENTICATE @expression ]
    [ DURATION
    [ FOR GRANT @duration ]
    [ FOR TOKEN @duration ]
    [ FOR SESSION @duration ]
    ]
    """

    def __init__(self, connection: Connection, name: str):
        super().__init__(connection)
        self._name = name
        self._if_not_exists = None
        self._duration: List[Optional[str]] = [None, None, None]
        self._auth = None
        self._user_type = None

    def if_not_exists(self) -> "DefineAccessBearer":
        """
        Represents the IF NOT EXISTS statement.
        :return: DefineAccessJwt object
        """
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineAccessBearer":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

    def type_record(self) -> "DefineAccessBearer":
        """
        Sets the type of access to record
        :return: DefineAccessJwt object
        """
        self._user_type = False
        return self

    def type_user(self) -> "DefineAccessBearer":
        """
        Sets the type of access to user
        :return: DefineAccessJwt object
        """
        self._user_type = True
        return self

    def authenticate(self, raw_expression: str) -> "DefineAccessBearer":
        """
        Represents the AUTHENTICATE clause in a final statement. Expression will be inserted as is.

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/access/jwt#with-authenticate-clause

        """
        self._auth = raw_expression
        return self

    def duration_for_grant(self, duration: str) -> "DefineAccessBearer":
        """
        Sets the duration for grant
        :param duration: duration
        :return: DefineAccessJwt object
        """
        self._duration[0] = duration
        return self

    def duration_for_token(self, duration: str) -> "DefineAccessBearer":
        """
        Sets the duration for grant
        :param duration: duration
        :return: DefineAccessJwt object
        """
        self._duration[1] = duration
        return self

    def duration_for_session(self, duration: str) -> "DefineAccessBearer":
        """
        Sets the duration for grant
        :param duration: duration
        :return: DefineAccessJwt object
        """
        self._duration[2] = duration
        return self

    def validate(self) -> List[str]:
        if self._duration != [None, None, None]:
            for duration in self._duration:
                if duration:
                    if not any(duration.endswith(letter) for letter in ('h', 'd', 'w', 'm', 'y')):
                        return [f"Wrong duration {duration}, allowed postfix are ('h', 'd', 'w', 'm', 'y')"]
                    if ' ' in duration:
                        return ["Wrong duration format, should be like 5m"]
        return [OK]

    def _clean_str(self):
        exists = ""
        if self._if_not_exists:
            exists = " IF NOT EXISTS"
        elif self._if_not_exists is False:
            exists = " OVERWRITE"
        auth = "" if not self._auth else f" AUTHENTICATE {self._auth}"
        durations = []
        if self._duration[0]:
            durations.append(f"FOR GRANT {self._duration[0]}")
        if self._duration[1]:
            durations.append(f"FOR TOKEN {self._duration[1]}")
        if self._duration[2]:
            durations.append(f"FOR SESSION {self._duration[2]}")
        duration = f" DURATION {', '.join(durations)}" if durations else ""
        kind = ""
        if self._user_type:
            kind = " FOR USER"
        elif self._user_type is False:
            kind = " FOR RECORD"
        return f"DEFINE ACCESS{exists} {self._name} ON DATABASE TYPE BEARER{kind}{auth}{duration}"


class DefineAccessJwt(Statement):
    """
    A JWT access method allows accessing SurrealDB with a token signed by a trusted issuer.
    The contents of the token will be trusted by SurrealDB as long as it has been signed with a trusted credential.

    Refer to: https://surrealdb.com/docs/surrealql/statements/define/access/jwt

    DEFINE ACCESS [ OVERWRITE | IF NOT EXISTS ] @name
    ON [ NAMESPACE | DATABASE ]
    TYPE JWT [ ALGORITHM @algorithm KEY @key | URL @url ]
    [ AUTHENTICATE @expression ]
    [ DURATION FOR SESSION @duration ]
    """

    def __init__(self, connection: Connection, name: str):
        super().__init__(connection)
        self._name = name
        self._if_not_exists = None
        self._algo = None
        self._duration = None
        self._url = None
        self._auth = None

    def if_not_exists(self) -> "DefineAccessJwt":
        """
        Represents the IF NOT EXISTS statement.
        :return: DefineAccessJwt object
        """
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineAccessJwt":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

    def authenticate(self, raw_expression: str) -> "DefineAccessJwt":
        """
        Represents the AUTHENTICATE clause in a final statement. Expression will be inserted as is.

        Refer to:
        https://surrealdb.com/docs/surrealdb/surrealql/statements/define/access/jwt#with-authenticate-clause

        """
        self._auth = raw_expression
        return self

    def algorithm(self, algorithm: Algorithm, key: str) -> "DefineAccessJwt":
        """
        Represents the ALGORITHM clause in a final statement. If this method is called, the URL statement is ignored.

        :param algorithm: The algorithm to use, see :class:`surrealist.enums.Algorithm`
        :param key: The key to use
        :return: DefineAccessJwt object
        """
        self._algo = (algorithm, key)
        self._url = None
        return self

    def url(self, url: str) -> "DefineAccessJwt":
        """
        Represents the URL clause in a final statement. If this method is called, the ALGORITHM statement is ignored.
        """
        self._algo = None
        self._url = url
        return self

    def duration(self, duration: str) -> "DefineAccessJwt":
        """
        Represents the DURATION FOR SESSION clause in a final statement.
        Allowed values are: "1h", "1d", "1w", "1m", "1y"
        """
        self._duration = duration
        return self

    def validate(self) -> List[str]:
        if self._duration:
            if not any(self._duration.endswith(letter) for letter in ('h', 'd', 'w', 'm', 'y')):
                return [f"Wrong duration {self._duration}, allowed postfix are ('h', 'd', 'w', 'm', 'y')"]
            if ' ' in self._duration:
                return ["Wrong duration format, should be like 5m"]
        return [OK]

    def _clean_str(self):
        exists = ""
        if self._if_not_exists:
            exists = " IF NOT EXISTS"
        elif self._if_not_exists is False:
            exists = " OVERWRITE"
        access = ""
        if self._algo:
            access = f" ALGORITHM {self._algo[0].name} KEY '{self._algo[1]}'"
        if self._url:
            access = f" URL '{self._url}'"
        auth = "" if not self._auth else f" AUTHENTICATE {self._auth}"
        duration = f" DURATION FOR SESSION {self._duration}" if self._duration else ""
        return f"DEFINE ACCESS{exists} {self._name} ON DATABASE TYPE JWT{access}{auth}{duration}"


class DefineAccessRecord(Statement):
    """
    A record access method allows accessing SurrealDB as a record user.
    Record users allow SurrealDB to operate as a web database by offering mechanisms to define custom signin and signup
    logic as well as custom table and field permissions.

    Refer to: https://surrealdb.com/docs/surrealql/statements/define/access/record

        DEFINE ACCESS [ OVERWRITE | IF NOT EXISTS ] @name
      ON DATABASE TYPE RECORD
        [ SIGNUP @expression ]
        [ SIGNIN @expression ]
        [ AUTHENTICATE @expression ]
        [ WITH JWT
          [ ALGORITHM @algorithm KEY @key | URL @url ]
          [ WITH ISSUER KEY @key ]
        ]
      [ DURATION
        [ FOR TOKEN @duration ]
        [ FOR SESSION @duration ]
      ]
    """

    def __init__(self, connection: Connection, name: str):
        super().__init__(connection)
        self._name = name
        self._if_not_exists = None
        self._algo = None
        self._duration_token = None
        self._duration_session = None
        self._url = None
        self._signup = None
        self._signin = None
        self._auth = None

    def if_not_exists(self) -> "DefineAccessRecord":
        """
        Represents the IF NOT EXISTS statement.
        :return: DefineAccessJwt object
        """
        self._if_not_exists = True
        return self

    def overwrite(self) -> "DefineAccessRecord":
        """
        Adds OVERWRITE statement to the query
        :return: self
        """
        self._if_not_exists = False
        return self

    def signup(self, expression: Union[str, Create]) -> "DefineAccessRecord":
        """
        Represents the SIGNUP clause in a final statement. Can be raw string or Create object
        """
        self._signup = expression
        return self

    def signin(self, expression: Union[str, Select]) -> "DefineAccessRecord":
        """
        Represents the SIGNUP clause in a final statement. Can be raw string or Select object
        """
        self._signin = expression
        return self

    def authenticate(self, raw_expression: str) -> "DefineAccessRecord":
        """
        Represents the AUTHENTICATE clause in a final statement. Expression will be inserted as is.

        Refer to:
        https://surrealdb.com/docs/surrealql/statements/define/access/record#with-authenticate-clause


        """
        self._auth = raw_expression
        return self

    def algorithm(self, algorithm: Algorithm, key: str, issuer_key: Optional[str] = None) -> "DefineAccessRecord":
        """
        Represents the ALGORITHM clause in a final statement. If this method is called, the URL statement is ignored.

        :param algorithm: The algorithm to use, see :class:`surrealist.enums.Algorithm`
        :param key: The key to use
        :param issuer_key: The issuer key to use
        :return: DefineAccessRecord object
        """
        self._algo = (algorithm, key, issuer_key)
        self._url = None
        return self

    def url(self, url: str, issuer_key: Optional[str] = None) -> "DefineAccessRecord":
        """
        Represents the URL clause in a final statement. If this method is called, the ALGORITHM statement is ignored.
        :param url: The URL to use
        :param issuer_key: The issuer key to use
        :return: DefineAccessRecord object
        """
        self._algo = None
        self._url = (url, issuer_key)
        return self

    def duration_for_token(self, duration: str) -> "DefineAccessRecord":
        """
        Represents the DURATION FOR TOKEN clause in a final statement.
        Allowed values are: "1h", "1d", "1w", "1m", "1y"
        """
        self._duration_token = duration
        return self

    def duration_for_session(self, duration: str) -> "DefineAccessRecord":
        """
        Represents the DURATION FOR SESSION clause in a final statement.
        Allowed values are: "1h", "1d", "1w", "1m", "1y"
        """
        self._duration_session = duration
        return self

    def validate(self) -> List[str]:
        for duration in (self._duration_token, self._duration_session):
            if duration:
                if not any(duration.endswith(letter) for letter in ('h', 'd', 'w', 'm', 'y')):
                    return [f"Wrong duration {duration}, allowed postfix are ('h', 'd', 'w', 'm', 'y')"]
                if ' ' in duration:
                    return ["Wrong duration format, should be like 5m"]
        return [OK]

    def _clean_str(self):
        exists = ""
        if self._if_not_exists:
            exists = " IF NOT EXISTS"
        elif self._if_not_exists is False:
            exists = " OVERWRITE"
        sin = ""
        if self._signin:
            sin = f" SIGNIN {self._signin}" if isinstance(self._signin, str) else f" SIGNIN {self._signin._clean_str()}"
        sup = ""
        if self._signup:
            sup = f" SIGNUP {self._signup}" if isinstance(self._signup, str) else f" SIGNUP {self._signup._clean_str()}"
        access = ""
        if self._algo:
            access = f" ALGORITHM {self._algo[0].name} KEY '{self._algo[1]}'"
            if self._algo[2]:
                access = f"{access} WITH ISSUER KEY '{self._algo[2]}'"
        duration_t = f"DURATION FOR TOKEN {self._duration_token}" if self._duration_token else ""
        duration_s = f"DURATION FOR SESSION {self._duration_session}" if self._duration_session else ""
        duration = f' {", ".join((duration_t, duration_s))}'
        if not duration_t and not duration_s:
            duration = ""
        if self._url:
            access = f" URL '{self._url[0]}'"
            if self._url[1]:
                access = f"{access} WITH ISSUER KEY '{self._url[1]}'"
        auth = "" if not self._auth else f" AUTHENTICATE {self._auth}"
        return f"DEFINE ACCESS{exists} {self._name} ON DATABASE TYPE RECORD{sin}{sup}{auth}{access}{duration}"
