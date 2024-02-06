class PySurrealError(Exception):
    """
    Parent for all types of errors in surrealist, so you can use more broad error in except clauses

    try:
        code() # some job with connection
    except PySurrealError:
        report() # do something on error
    """


class SurrealPermissionsError(PySurrealError):
    pass


class SurrealConnectionError(PySurrealError):
    """
    Raises on any problem with connection to SurrealDB, check url and other parameters
    """


class HttpClientError(PySurrealError):
    """
    Raises on http-client specific errors, in normal situations user should never see this
    """


class HttpConnectionError(PySurrealError):
    """
    Raises on troubles with connection or authentication, when using http-connection
    """


class WebSocketConnectionError(PySurrealError):
    """
    Raises on troubles with connection or data, when using websocket-connection
    """


class WebSocketConnectionClosedError(PySurrealError):
    """
    Raises only if websocket connection was closed, but one or more websocket clients still alive
    (waiting for some data)
    """


class ConnectionParametersError(PySurrealError):
    """
    Raises only if websocket connection gets wrong namespace/database parameters, in normal situations user
    should never see this
    """


class OperationOnClosedConnectionError(PySurrealError):
    """
    Raises on attempt to use already closed connection
    """


class CompatibilityError(PySurrealError):
    """
    Raises on attempt to use methods incompatible for that transport (websocket ot http), for example **live** do not
    work on http-connection.
    More on this here: https://github.com/kotolex/py_surreal?tab=readme-ov-file#transports
    """


class TooManyNestedLevelsError(PySurrealError):
    """
    Raises when str or json cant handle the object because of too deep nesting and recursion limit in python
    See documentation: https://github.com/kotolex/py_surreal?tab=readme-ov-file#recursion-and-json-in-python
    """
