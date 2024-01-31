import dataclasses
import json
import re
import uuid
from typing import Union, List, Dict, Optional

ENCODING = "UTF-8"
OK = "OK"
DEFAULT_TIMEOUT = 5  # timeout in seconds for basic operations
DATA_LENGTH_FOR_LOGS = 300  # size of data in logs, data will be cropped if bigger than that


def _set_length(length: int):
    global DATA_LENGTH_FOR_LOGS
    DATA_LENGTH_FOR_LOGS = length


def get_uuid() -> str:
    return str(uuid.uuid4())


def mask_pass(text: str) -> str:
    """
    Mask the passwords in logs. Replace all 'pass':'any_pass' in logs to 'pass':'******'/ Works both with ' and "
    :param text: text before put it to logs
    :return: text without visible passwords
    """
    return re.sub(r"(?ms)(?<=['\"]pass['\"]: ['\"]).*?(?=['\"])", '******', text)


def crop_data(data: Union[str, bytes], is_str: bool = True) -> Union[str, bytes]:
    """
    Crop the data to maximum size, no actions if data is smaller, work with str and bytes in logs

    :param data: str or bytes of data to put it in logs
    :param is_str: flag to use correct operation
    :return: data of same type, but cropped if it bigger, than DATA_LENGTH_FOR_LOGS
    """
    if len(data) > DATA_LENGTH_FOR_LOGS:
        return f"{data[:DATA_LENGTH_FOR_LOGS]}..." if is_str else data[:DATA_LENGTH_FOR_LOGS] + b'...'
    return data


@dataclasses.dataclass
class SurrealResult:
    """
    Represent common object for all possible responses of SurrealDB
    """
    id: Optional[Union[int, str]] = None
    error: Optional[Dict] = None
    result: Optional[Union[str, Dict, List]] = None
    code: Optional[int] = None
    token: Optional[str] = None
    status: Optional[str] = OK
    time: str = ''

    def is_error(self):
        """
        Method to know the error was returned

        :return: True if response contains error, False otherwise
        """
        return self.error is not None


def to_result(content: Union[str, Dict]) -> SurrealResult:
    """
    Converts str or dict response of SurrealDB to common object for convenient use

    :param content: response from SurrealDB
    :return: Result object
    """
    if isinstance(content, str):
        content = json.loads(content)
    if isinstance(content, List):
        content = content[0]
    if 'details' in content:
        content['result'] = content['details']
        del content['details']
    return SurrealResult(**content)
