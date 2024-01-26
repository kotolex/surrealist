import json
from datetime import datetime
from typing import Tuple, Dict, Optional

from py_surreal.http_client import HttpClient
from py_surreal.const import ENCODING, to_db_result, DbResult, AuthResult, to_auth_result, get_uuid


class Surreal:
    def __init__(self, url: str, namespace: str, database: str, credentials: Tuple[str, str] = None,
                 use_http: bool = True):
        client = HttpClient if use_http else list  # ws_client here
        self.namespace = namespace
        self.database = database
        hs = {"NS": namespace, "DB": database}
        self.client = client(url, headers=hs, credentials=credentials)
        self.http_client = HttpClient(url, headers=hs, credentials=credentials)

    def is_ready(self) -> bool:
        return self.status() == "OK"

    def status(self) -> str:
        code, text = self._simple_get("health")
        return "OK" if code == 200 else text

    def health(self) -> str:
        code, text = self._simple_get("health")
        return "OK" if code == 200 else text

    def version(self) -> str:
        return self._simple_get("version")[1]

    def export(self) -> str:
        _, text = self._check(self._simple_get("export"))
        return text

    def all_records_at(self, name: str) -> DbResult:
        _, text = self._check(self._simple_get(f"key/{name}"))
        return to_db_result(text)

    def record_by_id(self, table_name: str, record_id: str) -> DbResult:
        _, text = self._check(self._simple_get(f"key/{table_name}/{record_id}"))
        return to_db_result(text)

    def ml_export(self, name: str, version: str) -> str:
        _, text = self._check(self._simple_get(f"ml/export/{name}/{version}"))
        return text

    def signin(self, user: str, password: str, namespace: Optional[str] = None,
               database: Optional[str] = None) -> AuthResult:
        ns = namespace or self.namespace
        db = database or self.database
        body = {"ns": ns, "db": db, "user": user, "pass": password}
        _, text = self._check(self._simple_post("signin", body))
        return to_auth_result(text)

    def new_record_at(self, table_name: str, data: Dict, record_id: Optional[str] = None) -> DbResult:
        body = data if not record_id else {"id": get_uuid(), **data}
        _, text = self._check(self._simple_post(f"key/{table_name}", body))
        return to_db_result(text)

    def change_all_at(self, table_name: str, data: Dict) -> DbResult:
        _, text = self._check(self._simple_put(f"key/{table_name}", data))
        return to_db_result(text)

    def _simple_get(self, endpoint: str) -> Tuple[int, str]:
        with self.http_client.get(endpoint) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            return status, text

    def _simple_post(self, endpoint: str, data: Dict) -> Tuple[int, str]:
        with self.http_client.post(data, endpoint) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            return status, text

    def _simple_put(self, endpoint: str, data: Dict) -> Tuple[int, str]:
        with self.http_client.put(data, endpoint) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            return status, text

    def _check(self, result: Tuple[int, str]):
        status, text = result
        if status != 200:
            raise ValueError(f"Problem with db connect, check parameters! Info: code {status}, text: {text}")
        return result


if __name__ == '__main__':
    sur = Surreal("http://127.0.0.1:8000/", "test", "test", credentials=('root', 'root'))
    print(sur.is_ready())
    print(sur.status())
    print(sur.health())
    print(sur.version())
    print(sur.all_records_at("article"))
    print(sur.record_by_id("article2", "fbk43xn5vdb026hdscnz"))
    print(sur.signin("root", "root"))
    dt = {'created_at': str(datetime.now()),
          'author': 'author:john',
          'title': 'Lorem ipsum dolor13',
          'text': 'Donec eleifend, nunc vitae commodo accumsan, mauris est fringilla.'}
    print(sur.new_record_at("article", dt))
    dt = {'created_at': str(datetime.now()),
          'author': 'author:john',
          'title': 'New title', }
    print(sur.change_all_at("article", dt))
