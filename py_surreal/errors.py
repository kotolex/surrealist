class PySurrealError(Exception):
    """
    Parent for all types of errors in py_surreal, so you can use more broad error in except clauses

    try:
        // some job with connection
    except PySurrealError:
        // do something on error
    """
    # TODO make code pretty-formatted
    pass


class SurrealConnectionError(PySurrealError):
    """
    Raises on any problem with connection to SurrealDB, check url and other parameters
    """
    pass


class HttpClientError(PySurrealError):
    """
    Raises on http-client specific errors, in normal situations user should never see this
    """
    pass


class HttpConnectionError(PySurrealError):
    """
    Raises on troubles with connection or authentication, when using http-connection
    """
    pass


class WebSocketConnectionError(PySurrealError):
    """
    Raises on troubles with connection or data, when using websocket-connection
    """
    pass


class WebSocketConnectionClosedError(PySurrealError):
    """
    Raises only if websocket connection was closed, but one or more websocket clients still alive
    (waiting for some data)
    """
    pass


class ConnectionParametersError(PySurrealError):
    """
    Raises only if websocket connection gets wrong namespace/database parameters, in normal situations user
    should never see this
    """
    pass


class OperationOnClosedConnectionError(PySurrealError):
    """
    Raises on attempt to use already closed connection
    """
    pass


class CompatibilityError(PySurrealError):
    """
    Raises on attempt to use methods for that client  incompatible (websocket ot http), for example **live** do not
    work on http-connection.
    More on this here: url
    """
    # TODO link here
    pass
