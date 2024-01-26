import dataclasses
import json
import uuid
from typing import Union, List, Dict

ENCODING = "UTF-8"


@dataclasses.dataclass
class DbResult:
    result: Union[List, Dict]
    status: str
    time: str


@dataclasses.dataclass
class AuthResult:
    code: int
    details: str
    token: str


def to_db_result(content: str) -> DbResult:
    result = json.loads(content)[0]
    return DbResult(result['result'], result['status'], result['time'])


def to_auth_result(content: str) -> AuthResult:
    result = json.loads(content)
    return AuthResult(result['code'], result['details'], result['token'])


def get_uuid():
    return str(uuid.uuid4())
