class SurrealConnectionError(Exception):
    pass


class HttpClientError(Exception):
    pass


class HttpConnectionError(Exception):
    pass


class WebSocketConnectionError(Exception):
    pass


class WebSocketConnectionClosed(Exception):
    pass


class ConnectionParametersError(Exception):
    pass


class OperationOnClosedConnectionError(Exception):
    pass


class CompatibilityError(Exception):
    pass
