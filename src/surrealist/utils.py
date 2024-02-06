import re
import uuid
from typing import Union

ENCODING = "UTF-8"
OK = "OK"
ERR = "ERR"
HTTP_OK = 200  # status code for success
DEFAULT_TIMEOUT = 5  # timeout in seconds for basic operations
DATA_LENGTH_FOR_LOGS = 300  # size of data in logs, data will be cropped if bigger than that


def _set_length(length: int):
    global DATA_LENGTH_FOR_LOGS
    DATA_LENGTH_FOR_LOGS = length


def get_uuid() -> str:
    """
    Helper to generate uuid for records or testing purposes

    :return: uuid string representation
    """
    return str(uuid.uuid4())


def mask_pass(text: str) -> str:
    """
    Mask the passwords in logs. Replace all 'pass':'any_pass' in logs to 'pass':'******'. Works both with ' and "
    :param text: text before putting it to log
    :return: text without visible passwords
    """
    return re.sub(r"(?ms)(?<=['\"]pass['\"]: ['\"]).*?(?=['\"])", '******', text)


def crop_data(data: Union[str, bytes], is_str: bool = True) -> Union[str, bytes]:
    """
    Crop the data to maximum size, no actions if data is smaller, work with str and bytes in logs

    :param data: str or bytes of data to put it in the logs
    :param is_str: a flag to use correct operation
    :return: data of a same type, but cropped if it is bigger than DATA_LENGTH_FOR_LOGS
    """
    if len(data) > DATA_LENGTH_FOR_LOGS:
        return f"{data[:DATA_LENGTH_FOR_LOGS]}..." if is_str else data[:DATA_LENGTH_FOR_LOGS] + b'...'
    return data
