from typing import List, Optional, Union

from surrealist.connections.connection import Connection
from surrealist.enums import Algorithm
from surrealist.ql.statements import Create, Select
from surrealist.ql.statements.statement import Statement
from surrealist.utils import OK


class DefineAccessJwt(Statement):
    """
    A JWT access method allows accessing SurrealDB with a token signed by a trusted issuer.
    The contents of the token will be trusted by SurrealDB as long as it has been signed with a trusted credential.

    Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/access/jwt

    DEFINE ACCESS [ IF NOT EXISTS ] @name
    ON [ NAMESPACE | DATABASE ]
    TYPE JWT [ ALGORITHM @algorithm KEY @key | URL @url ]
    [ DURATION FOR SESSION @duration ]
    """

    def __init__(self, connection: Connection, name: str):
        super().__init__(connection)
        self._name = name
        self._if_not_exists = False
        self._algo = None
        self._duration = None
        self._url = None

    def if_not_exists(self) -> "DefineAccessJwt":
        """
        Represents the IF NOT EXISTS statement.
        :return: DefineAccessJwt object
        """
        self._if_not_exists = True
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
        exists = " IF NOT EXISTS" if self._if_not_exists else ""
        access = ""
        if self._algo:
            access = f" ALGORITHM {self._algo[0].name} KEY '{self._algo[1]}'"
        if self._url:
            access = f" URL '{self._url}'"
        duration = f" DURATION FOR SESSION {self._duration}" if self._duration else ""
        return f"DEFINE ACCESS{exists} {self._name} ON DATABASE TYPE JWT{access}{duration}"


class DefineAccessRecord(Statement):
    """
    A record access method allows accessing SurrealDB as a record user.
    Record users allow SurrealDB to operate as a web database by offering mechanisms to define custom signin and signup
    logic as well as custom table and field permissions.

    Refer to: https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/access/record

        DEFINE ACCESS [ IF NOT EXISTS ] @name
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
        self._if_not_exists = False
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
        https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/access/record#with-authenticate-clause


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
        exists = " IF NOT EXISTS" if self._if_not_exists else ""
        sin = ""
        if self._signin:
            if isinstance(self._signin, str):
                sin = f" SIGNIN {self._signin}"
            else:
                sin = f" SIGNIN {self._signin._clean_str()}"
        sup = ""
        if self._signup:
            if isinstance(self._signup, str):
                sup = f" SIGNUP {self._signup}"
            else:
                sup = f" SIGNUP {self._signup._clean_str()}"
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
