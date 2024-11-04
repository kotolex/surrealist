from .update import Update


class Upsert(Update):
    """
    Represents an UPSERT statement.

    The UPSERT statement can be used to modify records in the database if they already exist. If the record does not
    exist, it will be created.
    This is different from the UPDATE statement, which will fail if the record does not exist.

    Refer to: https://surrealdb.com/docs/surrealql/statements/upsert

    UPSERT [ ONLY ] @targets
    [ CONTENT @value
      | MERGE @value
      | PATCH @value
      | SET @field = @value ...
    ]
    [ WHERE @condition ]
    [ RETURN NONE | RETURN BEFORE | RETURN AFTER | RETURN DIFF | RETURN @statement_param, ... ]
    [ TIMEOUT @duration ]
    [ PARALLEL ];
    """

    def _clean_str(self):
        return f"UPSERT{super()._clean_str().lstrip('UPDATE')}"
