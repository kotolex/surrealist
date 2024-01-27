import dataclasses
import json
import uuid
from typing import Union, List, Dict, Tuple

from py_surreal.errors import HttpConnectionError

ENCODING = "UTF-8"
OK = "OK"
DEFAULT_TIMEOUT = 5


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


@dataclasses.dataclass
class Result:
    id_: Union[int, str]

    def is_error(self):
        return False


@dataclasses.dataclass
class DbError(Result):
    error: Dict

    def is_error(self):
        return True


@dataclasses.dataclass
class DbSimpleResult(Result):
    result: str


def to_db_result(content: str) -> DbResult:
    result = json.loads(content)[0]
    return DbResult(result['result'], result['status'], result['time'])


def to_auth_result(content: str) -> AuthResult:
    result = json.loads(content)
    return AuthResult(result['code'], result['details'], result['token'])


def ws_message_to_result(message: Dict) -> Result:
    if 'error' in message:
        return DbError(message['id'], message['error'])
    return DbSimpleResult(message['id'], message['result'])


def raise_if_not_http_ok(result: Tuple[int, str]):
    status, text = result
    if status != 200:
        raise HttpConnectionError(f"Status code is {status}, check your data! Info:\n{text}")
    return result
