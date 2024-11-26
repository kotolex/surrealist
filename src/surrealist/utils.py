import datetime
import json
import re
import uuid
from typing import Union, Dict, Optional, List, Tuple, Any

from .errors import SurrealRecordIdError
from .record_id import RecordId

StrOrRecord = Union[str, RecordId]  # Represents a string or a RecordId for queries
StrOrInt = Union[str, int]  # Represents datetime or versionstamp for SHOW statement

ENCODING = "UTF-8"
OK = "OK"
ERR = "ERR"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
DATE_FORMAT_NS = "%Y-%m-%dT%H:%M:%S.%fZ"
HTTP_OK = 200  # status code for success
DEFAULT_TIMEOUT = 15  # timeout in seconds for basic operations
LOG_FORMAT = '%(asctime)s : %(threadName)s : %(name)s : %(levelname)s : %(message)s'  # use it for logs
NS = "NS"
DB = "DB"
AC = "AC"


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


def to_surreal_datetime_str(dt: datetime.datetime) -> str:
    """
    Convert datetime to string in Surreal format, for example: d'2024-04-18T11:34:41.665249Z'
    :param dt: datetime object
    :return: string representation of the datetime
    """
    return f"d'{dt.strftime(DATE_FORMAT_NS)}'"


def to_datetime(dt_str: str) -> datetime.datetime:
    """
    Convert string in Surreal format to datetime object
    :param dt_str: string datetime representation in Surreal format (e.g. d'2024-04-18T11:34:41.665249Z')
    :return: datetime object
    """
    if "d'" in dt_str:
        dt_str = dt_str.rstrip("'").replace("d'", "")
    elif 'd"' in dt_str:
        dt_str = dt_str.rstrip('"').replace('d"', '')
    return datetime.datetime.strptime(dt_str, DATE_FORMAT_NS)


def clean_dates(data: str) -> str:
    """
    Get surreal dates with d' prefix out of quotes
    :param data: some Surreal query
    :return: data with valid Surreal dates
    """
    return re.sub(r'["\'](d(["\']).+?)["\']+', r'\1\2', data)


def safe_dumps(data: Any) -> str:
    """
    Convert data to json string with special logic for RecordId (if it exists)
    We have to do it because since version 2.0 of SurrealDB it never converts string to record_id
    :param data: data to convert, dict, list, tuple or JSON serializable object expected here
    :return: string
    """
    if isinstance(data, RecordId):
        return data.to_valid_string()
    if isinstance(data, List):
        return list_to_json_str(data)
    if isinstance(data, Tuple):
        return tuple_to_json_str(data)
    if isinstance(data, Dict):
        return dict_to_json_str(data)
    return json.dumps(data)


def dict_to_json_str(data: Dict) -> str:
    """
    Convert dict to json string with special logic for RecordId (if it exists)
    We have to do it because since version 2.0 of SurrealDB it never converts string to record_id
    :param data: dict to convert
    :return: string
    """
    ids = {k: v for k, v in data.items() if isinstance(v, (RecordId, List, Dict))}
    if not ids:
        return json.dumps(data)
    without_ids = {k: v for k, v in data.items() if k not in ids}
    first = json.dumps(without_ids) if without_ids else ""
    join = ", ".join([f'"{k}": {safe_dumps(v)}' for k, v in ids.items()])
    if first:
        first = first.rstrip("}")
        first = f"{first}, "
    else:
        first = "{"
    return f"{first}{join}}}"


def list_to_json_str(data: List) -> str:
    """
    Convert a list to json string with special logic for RecordId (if it exists)
    We have to do it because since version 2.0 of SurrealDB it never converts string to record_id
    :param data: list of pairs to convert
    :return: string
    """
    ids = [e for e in data if isinstance(e, (RecordId, List, Dict, Tuple))]
    if not ids:
        return json.dumps(data)
    without_ids = [e for e in data if e not in ids]
    first = json.dumps(without_ids) if without_ids else ""
    join = ", ".join(safe_dumps(e) for e in ids)
    if first:
        first = first.rstrip("]")
        first = f"{first}, "
    else:
        first = "["
    return f"{first}{join}]"


def tuple_to_json_str(values: Tuple) -> str:
    """
    Convert a tuple to json string with special logic for RecordId (if it exists)
    We have to do it because since version 2.0 of SurrealDB it never converts string to record_id
    :param values: tuple to convert
    :return: string
    """
    ids = [e for e in values if isinstance(e, (RecordId, List, Dict, Tuple))]
    if not ids:
        return f'({json.dumps(values).lstrip("[").rstrip("]")})'
    without_ids = [e for e in values if e not in ids]
    first = json.dumps(without_ids)
    first = first.lstrip("[").rstrip("]")
    if first:
        first = f"{first}, "
    join = ", ".join(f'{safe_dumps(e)}' for e in ids)
    return f"({first}{join})"


def get_table_or_record_id(table_name: str, record_id: Optional[StrOrRecord]) -> str:
    """
    A helper for backward compatibility and converting string to record_id
    :param table_name: name of table
    :param record_id: string or record_id
    :return: valid record_id representation
    """
    if record_id is not None:
        if isinstance(record_id, (str, int)):
            record_id = RecordId(str(record_id), table=table_name)
        part = record_id.table_part
        if table_name != part:
            raise SurrealRecordIdError(f"Table name is different from id, we expect {table_name}, but got {part}")
        return record_id.to_valid_string()
    return table_name
