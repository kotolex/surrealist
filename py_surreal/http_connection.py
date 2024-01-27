from typing import Tuple, Dict, Optional, Union

from py_surreal.const import ENCODING, to_db_result, DbResult, AuthResult, to_auth_result, get_uuid
from py_surreal.errors import SurrealConnectionError, HttpClientError, HttpConnectionError
from py_surreal.http_client import HttpClient


def check(result: Tuple[int, str]):
    status, text = result
    if status != 200:
        raise HttpConnectionError(f"Problem with db connect, check parameters! Info: code {status}, text: {text}")
    return result


class HttpConnection:
    def __init__(self, url: str, namespace: str, database: str, credentials: Tuple[str, str] = None, timeout: int = 5):
        self.namespace = namespace
        self.database = database
        hs = {"NS": namespace, "DB": database}
        self.http_client = HttpClient(url, headers=hs, credentials=credentials, timeout=timeout)
        try:
            is_ready = self.is_ready()
        except HttpClientError:
            is_ready = False
        if not is_ready:
            raise SurrealConnectionError(f"Cant connect to {url} OR /status and /health are not OK.\n"
                                         f"Is your SurrealDB started and work on that url? "
                                         f"Refer to https://docs.surrealdb.com/docs/introduction/start")

    def is_ready(self) -> bool:
        return self.status() == "OK" and self.health() == "OK"

    def status(self) -> str:
        code, text = self._simple_get("status")
        return "OK" if code == 200 else text

    def health(self) -> str:
        code, text = self._simple_get("health")
        return "OK" if code == 200 else text

    def version(self) -> str:
        return self._simple_get("version")[1]

    def export(self) -> str:
        _, text = check(self._simple_get("export"))
        return text

    def select(self, table_name: str, record_id: Optional[str] = None) -> DbResult:
        url = f"key/{table_name}" if record_id is None else f"key/{table_name}/{record_id}"
        _, text = check(self._simple_get(url))
        return to_db_result(text)

    def ml_export(self, name: str, version: str) -> str:
        _, text = check(self._simple_get(f"ml/export/{name}/{version}"))
        return text

    def signin(self, user: str, password: str, namespace: Optional[str] = None,
               database: Optional[str] = None) -> AuthResult:
        ns = namespace or self.namespace
        db = database or self.database
        body = {"ns": ns, "db": db, "user": user, "pass": password}
        _, text = check(self._simple_request("POST", "signin", body))
        return to_auth_result(text)

    def new_record_at(self, table_name: str, data: Dict, record_id: Optional[str] = None,
                      db_generated_id: bool = False) -> DbResult:
        if record_id is None:
            if db_generated_id:
                url = f"key/{table_name}"
            else:
                _id = get_uuid()
                url = f"key/{table_name}/{_id}"
        else:
            url = f"key/{table_name}/{record_id}"
        _, text = check(self._simple_request("POST", url, data))
        return to_db_result(text)

    def change_all_at(self, table_name: str, data: Dict) -> DbResult:
        _, text = check(self._simple_request("PUT", f"key/{table_name}", data))
        return to_db_result(text)

    def change_record_at(self, table_name: str, data: Dict, record_id: str) -> DbResult:
        _, text = check(self._simple_request("PUT", f"key/{table_name}/{record_id}", data))
        return to_db_result(text)

    def delete_all_at(self, table_name: str) -> DbResult:
        _, text = check(self._simple_request("DELETE", f"key/{table_name}", {}))
        return to_db_result(text)

    def delete_record_at(self, table_name: str, record_id: str) -> DbResult:
        _, text = check(self._simple_request("DELETE", f"key/{table_name}/{record_id}", {}))
        return to_db_result(text)

    def patch_all_at(self, table_name: str, data: Dict) -> DbResult:
        _, text = check(self._simple_request("PATCH", f"key/{table_name}", data))
        return to_db_result(text)

    def patch_record_at(self, table_name: str, data: Dict, record_id: str) -> DbResult:
        _, text = check(self._simple_request("PATCH", f"key/{table_name}/{record_id}", data))
        return to_db_result(text)

    def query(self, query: str) -> DbResult:
        _, text = check(self._simple_request("POST", "sql", query, not_json=True))
        return to_db_result(text)

    def import_data(self, path) -> DbResult:
        file = open(path, 'rb')
        _, text = check(self._simple_request("POST", "import", file.read().decode(ENCODING), not_json=True))
        return to_db_result(text)

    def _simple_get(self, endpoint: str) -> Tuple[int, str]:
        with self.http_client.get(endpoint) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            return status, text

    def _simple_request(self, method, endpoint: str, data: Union[Dict, str], not_json: bool = False) -> Tuple[int, str]:
        with self.http_client.request(method, data, endpoint, not_json) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            return status, text


if __name__ == '__main__':
    sur = HttpConnection("http://127.0.0.1:8001/", "test", "test", credentials=('root', 'root'))
    print(sur.is_ready())
    print(sur.status())
    print(sur.health())
    print(sur.version())
