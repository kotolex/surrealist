import json
from typing import Optional, Union, Dict, List

from surrealist.errors import TooManyNestedLevelsError
from surrealist.utils import OK, ERR, HTTP_OK


class SurrealResult:
    """
    Represents result of the request both via http or websocket.
    """

    def __init__(self, **kwargs):
        """
        Expected fields:
        id - only from websocket requests
        result - error text or any other result of the request
        code - only from http requests
        query - text of the query if available
        status - OK or ERR, if ERR then result contains error text
        time - execution time, only for http requests
        additional_info -all other fields
        """
        self.id: Optional[Union[int, str]] = kwargs.pop("id", None)
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
        if self.result and ":" in self.result:
            self.result = self.result.split(":")[-1].strip()

    def is_error(self):
        """
        Method to know the error was returned

        :return: True if response contains error, False otherwise
        """
        return self.status != OK

    def __repr__(self):
        return f"SurrealResult(id={self.id}, status={self.status}, result={self.result}, query={self.query}, " \
               f"code={self.code}, time={self.time}, additional_info={self.additional_info})"

    def __eq__(self, other):
        if other is None or not isinstance(other, SurrealResult):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash((self.id, self.result, self.status, self.time, self.code, self.query))


def to_result(content: Union[str, Dict]) -> SurrealResult:
    """
    Converts str or dict response of SurrealDB to common object for convenient use

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
        res.id = content["id"]
        return res
    return SurrealResult(**content)


def _is_result_inside(a_dict) -> bool:
    """
    Helper predicate for deep nested objects
    """
    return len(a_dict) == 2 and set(a_dict.keys()) == {"id", "result"} and isinstance(a_dict["result"], List) \
        and len(a_dict["result"]) == 1 and set(a_dict["result"][0].keys()) == {"time", "status", "result"}
