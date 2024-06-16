import datetime
import re
import uuid
from typing import Union

ENCODING = "UTF-8"
OK = "OK"
ERR = "ERR"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_FORMAT_NS = "%Y-%m-%dT%H:%M:%S.%fZ"
HTTP_OK = 200  # status code for success
DEFAULT_TIMEOUT = 15  # timeout in seconds for basic operations
DATA_LENGTH_FOR_LOGS = 300  # size of data in logs, data will be cropped if bigger than that
LOG_FORMAT = '%(asctime)s : %(threadName)s : %(name)s : %(levelname)s : %(message)s'  # use it for logs
NS = "surreal-ns"
DB = "surreal-db"
AC = "ac"


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
    :return: data of the same type, but cropped if it is bigger than DATA_LENGTH_FOR_LOGS
    """
    if len(data) > DATA_LENGTH_FOR_LOGS:
        return f"{data[:DATA_LENGTH_FOR_LOGS]}..." if is_str else data[:DATA_LENGTH_FOR_LOGS] + b'...'
    return data


def to_surreal_datetime_str(dt: datetime.datetime) -> str:
    """
    Convert datetime to string in Surreal format, for example: 2024-04-18T11:34:41.665249Z
    :param dt: datetime object
    :return: string representation of the datetime
    """
    return dt.strftime(DATE_FORMAT_NS)


def to_datetime(dt_str: str) -> datetime.datetime:
    """
    Convert string in Surreal format to datetime object
    :param dt_str: string datetime representation in Surreal format (e.g. 2024-04-18T11:34:41.665249Z)
    :return: datetime object
    """
    return datetime.datetime.strptime(dt_str, DATE_FORMAT_NS)
