from enum import Enum, auto


class Algorithm(Enum):
    """
    Represents different types of cryptographic algorithms to use for token verification
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

