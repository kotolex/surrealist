from .surreal import Surreal
from .connections import WebSocketConnection, HttpConnection
from .errors import *
from .utils import DATA_LENGTH_FOR_LOGS

__version__ = "0.1.1"

__all__ = ("Surreal", "WebSocketConnection", "HttpConnection", "PySurrealError", "HttpConnectionError",
           "HttpClientError", "SurrealConnectionError", "WebSocketConnectionError", "WebSocketConnectionClosedError",
           "ConnectionParametersError", "CompatibilityError", "OperationOnClosedConnectionError",
           "DATA_LENGTH_FOR_LOGS")
