from typing import Optional

from .statement import Statement, FinishedStatement


class GrantFor(FinishedStatement):
    """
    Represents a GRANT [ FOR USER @name | FOR RECORD @record ] statement
    """

    def __init__(self, statement: Statement, is_user: bool, name: str):
        super().__init__(statement)
        self._is_user = is_user
        self._name = name

    def _clean_str(self):
        kind = "USER" if self._is_user else "RECORD"
        result = f"GRANT FOR {kind} {self._name}"
        return f"{self._statement._clean_str()} {result}"


class ShowGrant(FinishedStatement):
    """
    Represents a SHOW GRANT statement
    """

    def __init__(self, statement: Statement, id_: str):
        super().__init__(statement)
        self._id = id_

    def _clean_str(self):
        return f"{self._statement._clean_str()} SHOW GRANT {self._id}"


class RevokeGrant(FinishedStatement):
    """
    Represents a REVOKE GRANT statement
    """

    def __init__(self, statement: Statement, id_: str):
        super().__init__(statement)
        self._id = id_

    def _clean_str(self):
        return f"{self._statement._clean_str()} REVOKE GRANT {self._id}"


class ShowAll(FinishedStatement):
    """
    Represents a SHOW ALL statement
    """

    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} SHOW ALL"


class RevokeAll(FinishedStatement):
    """
    Represents a REVOKE ALL statement
    """

    def __init__(self, statement: Statement):
        super().__init__(statement)

    def _clean_str(self):
        return f"{self._statement._clean_str()} REVOKE ALL"


class ShowWhere(FinishedStatement):
    """
    Represents a SHOW WHERE statement
    """

    def __init__(self, statement: Statement, raw_expression: str):
        super().__init__(statement)
        self._raw_expression = raw_expression

    def _clean_str(self):
        return f"{self._statement._clean_str()} SHOW WHERE {self._raw_expression}"


class RevokeWhere(FinishedStatement):
    """
    Represents a REVOKE WHERE statement
    """

    def __init__(self, statement: Statement, raw_expression: str):
        super().__init__(statement)
        self._raw_expression = raw_expression

    def _clean_str(self):
        return f"{self._statement._clean_str()} REVOKE WHERE {self._raw_expression}"


class CanUseRevoke:
    """
    Represents interface for SHOW statement
    """

    def revoke_grant(self, id_: str) -> RevokeGrant:
        """
        Represents SHOW GRANT @id statement
        """
        return RevokeGrant(self, id_)

    def revoke_all(self) -> RevokeAll:
        """
        Represents SHOW ALL statement
        """
        return RevokeAll(self)

    def revoke_where(self, raw_expression: str) -> RevokeWhere:
        """
        Represents SHOW WHERE statement
        :param raw_expression: query expression, will be used as is
        """
        return RevokeWhere(self, raw_expression)


class CanUseShow:
    """
    Represents interface for SHOW statement
    """

    def show_grant(self, id_: str) -> ShowGrant:
        """
        Represents SHOW GRANT @id statement
        """
        return ShowGrant(self, id_)

    def show_all(self) -> ShowAll:
        """
        Represents SHOW ALL statement
        """
        return ShowAll(self)

    def show_where(self, raw_expression: str) -> ShowWhere:
        """
        Represents SHOW WHERE statement
        :param raw_expression: query expression, will be used as is
        """
        return ShowWhere(self, raw_expression)


class CanUseGrant:
    """
    Represents interface for GRANT statement
    """

    def grant_for_user(self, name: str) -> GrantFor:
        """
        Represents GRANT FOR USER @name
        """
        return GrantFor(self, True, name)

    def grant_for_record(self, name: str) -> GrantFor:
        """
        Represents FOR RECORD @record statement
        """
        return GrantFor(self, False, name)


class PurgeExpired(FinishedStatement):
    """
    Represents PURGE EXPIRED statement
    """

    def __init__(self, statement: Statement, for_duration: Optional[str] = None):
        super().__init__(statement)
        self._duration = for_duration

    def purge_revoked(self, for_duration: Optional[str] = None) -> "PurgeRevoked":
        """
        Represents PURGE REVOKED statement
        """
        return PurgeRevoked(self, for_duration, True)

    def _clean_str(self):
        revoked = " PURGE EXPIRED" if self._duration is None else f" PURGE EXPIRED {self._duration}"
        return f"{self._statement._clean_str()}{revoked}"


class PurgeRevoked(FinishedStatement):
    """
    Represents PURGE REVOKED statement
    """

    def __init__(self, statement: Statement, for_duration: Optional[str] = None, has_expired: bool = False):
        super().__init__(statement)
        self._duration = for_duration
        self._has_expired = has_expired

    def _clean_str(self):
        revoked = " PURGE REVOKED" if self._duration is None else f"PURGE REVOKED {self._duration}"
        if self._has_expired:
            revoked = f", {revoked}"
        return f"{self._statement._clean_str()}{revoked}"


class CanUsePurge:
    """
    Represents interface for PURGE statement
    """

    def purge_expired(self, for_duration: Optional[str] = None) -> PurgeExpired:
        """
        Represents PURGE EXPIRED statement
        """
        return PurgeExpired(self, for_duration)

    def purge_revoked(self, for_duration: Optional[str] = None) -> PurgeRevoked:
        """
        Represents PURGE REVOKED statement
        """
        return PurgeRevoked(self, for_duration)
