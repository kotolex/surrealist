import json
from typing import Optional, Union, Dict, List, Any

from surrealist.errors import TooManyNestedLevelsError, ResultHasNoValuesError
from surrealist.utils import OK, ERR, HTTP_OK


class SurrealResult:
    """
    Represents a result of the request both via http or websocket.
    Contains a few helpers to work with results of different kinds: id, ids, get, is_error, is_empty
    """

    def __init__(self, **kwargs):
        """
        Expected fields:
        id - only from websocket requests
        result - error text or any other result of the request
        code - http status code from http requests or error code from websocket
        query - text of the query if available
        status - OK or ERR, if ERR then result contains error text
        time - execution time, only for http requests
        additional_info -all other fields
        """
        self.ws_id: Optional[Union[int, str]] = kwargs.pop("id", None)
        self.result: Optional[Union[str, int, Dict, List]] = kwargs.pop("result", None)
        self.code: Optional[int] = kwargs.pop("code", None)
        self.query: Optional[str] = kwargs.pop("query", None)
        self.status: Optional[str] = kwargs.pop("status", OK)
        self.time: Optional[str] = kwargs.pop("time", None)
        if "information" in kwargs:
            self.result = kwargs.pop("information")
        if "error" in kwargs:
            self.result = kwargs.pop("error")
            self.status = ERR
        if "token" in kwargs:
            self.result = kwargs.pop("token")
        if self.code and self.code != HTTP_OK:
            self.status = ERR
        self.additional_info: Dict = kwargs
        if self.status == ERR and isinstance(self.result, dict) and "code" in self.result and "message" in self.result:
            self.code = self.result["code"]
            self.result = self.result["message"]
        if self.result and isinstance(self.result, str) and "There was a problem with the database:" in self.result:
            self.result = self.result.split(":", 1)[1].strip()

    def count(self) -> int:
        """
        Returns number of records in result.

        :return: int value of records in a result
        """
        if self.is_empty():
            return 0
        if isinstance(self.result, List):
            return len(self.result)
        return 1

    def is_empty(self) -> bool:
        """
        Return is result is empty

        :return: True, if a result is None, empty list, empty dict or empty string, False otherwise
        """
        return self.result in (None, [], {}, '')

    @property
    def id(self) -> str:
        """
        Returns id field from a result.
        Note: this property raise on any problem with getting id, sometimes it will be more effective to use ids instead

        :return: id from the record in result
        :raise ValueError: if a result is empty or no id in it
        """
        count = self.count()
        if count != 1:
            raise ValueError(f"No id at result or more than 1 element, body: {self.result}")
        if not isinstance(self.result, (List, Dict)):
            raise ValueError(f"Cant get id, body: {self.result}")
        value = self.result[0] if isinstance(self.result, List) else self.result
        if not isinstance(value, Dict) or "id" not in value:
            raise ValueError(f"No id field, body: {self.result}")
        return value["id"]

    @property
    def ids(self) -> List:
        """
        Returns list with all found id of records inside.
        Note: this property is never raise an error, instead it returns [] on an empty result or no id.
        Note: non-empty list returns None on each element without id, so [1,2,3] returns [None, None, None]

        :return: list with id of records inside
        """
        result = []
        if isinstance(self.result, Dict):
            if "id" in self.result:
                result.append(self.result["id"])
        if isinstance(self.result, List):
            result = [e.get("id") if isinstance(e, Dict) else None for e in self.result]
        return result

    def first(self):
        """
        Returns the first element of the result if it is a list. Otherwise, returns element itself, except when a result
        is empty

        :return: first element of the result, or result itself
        :raise ResultHasNoValuesError: if result is empty (None, [], {}, '')
        """
        if self.count() < 1:
            raise ResultHasNoValuesError("Cant get first on an empty result")
        if isinstance(self.result, List):
            return self.result[0]
        return self.result

    def last(self):
        """
        Returns the last element of the result if it is a list. Otherwise, returns element itself, except when a result
        is empty

        :return: last element of the result, or result itself
        :raise ResultHasNoValuesError: if result is empty (None, [], {}, '')
        """
        if self.count() < 1:
            raise ResultHasNoValuesError("Cant get first on an empty result")
        if isinstance(self.result, List):
            return self.result[-1]
        return self.result

    def get(self, field_name: str, default: Optional[Any] = None) -> Any:
        """
        Tries to get value by field name, will work with dict or only dict in a list, in all other cases returns default

        Examples:
        SurrealResult(result={"a":1}).get("a") == 1
        SurrealResult(result={"a":1}).get("b") == None
        SurrealResult(result=[{"a":1}]).get("a") == 1
        SurrealResult(result=[{"a":1}]).get("b") == None
        SurrealResult(result=[{"a":1}, {"a":2}]).get("a") == None # more than one dict in a result
        SurrealResult(result="token").get("a") == None # not a dict


        :param field_name: field name to look in a result
        :param default: value to return, when field not found
        :return: any result
        """
        count = self.count()
        if count != 1 or not isinstance(self.result, (List, Dict)):
            return default
        value = self.result[0] if isinstance(self.result, List) else self.result
        if not isinstance(value, Dict) or field_name not in value:
            return default
        return value[field_name]

    def is_error(self):
        """
        Method to know the error was returned

        :return: True if response contains error, False otherwise
        """
        return self.status != OK

    def to_dict(self) -> Dict:
        """
        Return all data as dict
        :return: dict with all fields
        """
        return {"ws_id": self.ws_id, "status": self.status, "result": self.result, "query": self.query,
                "code": self.code, "time": self.time, "additional_info": self.additional_info}

    def __repr__(self):
        return f"SurrealResult(id={self.ws_id}, status={self.status}, result={self.result}, query={self.query}, " \
               f"code={self.code}, time={self.time}, additional_info={self.additional_info})"

    def __eq__(self, other):
        if other is None or not isinstance(other, SurrealResult):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash((self.ws_id, self.result, self.status, self.time, self.code, self.query))


def to_result(content: Union[str, Dict, List]) -> SurrealResult:
    """
    Converts str or dict response of SurrealDB to a common object for convenient use

    :param content: response from SurrealDB
    :return: Result object
    """
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except RecursionError as exc:
            raise TooManyNestedLevelsError("Cant serialize object, too many nested levels") from exc
    if isinstance(content, List):
        if len(content) == 1:
            return SurrealResult(**content[0])
        return SurrealResult(result=[SurrealResult(**e) for e in content])
    if _is_result_inside(content):
        res = SurrealResult(**content["result"][0])
        res.ws_id = content["id"]
        return res
    return SurrealResult(**content)


def _is_result_inside(a_dict) -> bool:
    """
    Helper predicate for deep nested objects
    """
    return len(a_dict) == 2 and set(a_dict.keys()) == {"id", "result"} and isinstance(a_dict["result"], List) \
        and len(a_dict["result"]) == 1 and set(a_dict["result"][0].keys()) == {"time", "status", "result"}
