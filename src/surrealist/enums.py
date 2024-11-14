from enum import Enum, auto


class Transport(Enum):
    """
    Represents different types of transport for connection
    """
    HTTP = "http"
    WEBSOCKET = "websocket"


class Algorithm(Enum):
    """
    Represents different types of cryptographic algorithms to use for token verification
    Refer to: https://github.com/surrealdb/surrealdb/blob/main/core/src/iam/verify.rs#L17-L72
    """
    HS256 = auto()
    HS384 = auto()
    HS512 = auto()
    EDDSA = auto()
    ES256 = auto()
    ES384 = auto()
    ES512 = auto()
    PS256 = auto()
    PS384 = auto()
    PS512 = auto()
    RS256 = auto()
    RS384 = auto()
    RS512 = auto()

class AutoOrNone(Enum):
    """
    Represents the auto or none option for DEFINE CONFIG
    """
    AUTO = auto()
    NONE = auto()
