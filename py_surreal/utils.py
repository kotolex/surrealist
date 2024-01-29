import dataclasses
import json
import uuid
from typing import Union, List, Dict, Tuple, Optional

from py_surreal.errors import HttpConnectionError

ENCODING = "UTF-8"
OK = "OK"
DEFAULT_TIMEOUT = 5


def get_uuid():
    return str(uuid.uuid4())


@dataclasses.dataclass
class SurrealResult:
    id: Optional[Union[int, str]] = None
    error: Optional[Dict] = None
    result: Optional[Union[str, Dict, List]] = None
    code: Optional[int] = None
    token: Optional[str] = None
    status: Optional[str] = OK
    time: str = ''

    def is_error(self):
        return self.error is not None


def to_result(content: Union[str, Dict]) -> SurrealResult:
    if isinstance(content, str):
        content = json.loads(content)
    if isinstance(content, List):
        content = content[0]
    if 'details' in content:
        content['result'] = content['details']
        del content['details']
    return SurrealResult(**content)


def raise_if_not_http_ok(result: Tuple[int, str]):
    status, text = result
    text = text.replace(',', ',\n')
    if status != 200:
        raise HttpConnectionError(f"Status code is {status}, check your data! Info:\n{text}")
    return result
