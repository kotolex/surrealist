from .surreal import Surreal
from .ws_connection import WebSocketConnection
from .http_connection import HttpConnection
from .errors import *

__all__ = ("Surreal", "WebSocketConnection", "HttpConnection", "PySurrealError", "HttpConnectionError",
           "HttpClientError", "SurrealConnectionError", "WebSocketConnectionError", "WebSocketConnectionClosedError",
           "ConnectionParametersError", "CompatibilityError", "OperationOnClosedConnectionError")
