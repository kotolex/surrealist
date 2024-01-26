from typing import Tuple, Dict, Optional, Union

from py_surreal.const import ENCODING, to_db_result, DbResult, AuthResult, to_auth_result, get_uuid
from py_surreal.http_client import HttpClient


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
        _, text = self._check(self._simple_request("POST", "signin", body))
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
        _, text = self._check(self._simple_request("POST", url, data))
        return to_db_result(text)

    def change_all_at(self, table_name: str, data: Dict) -> DbResult:
        _, text = self._check(self._simple_request("PUT", f"key/{table_name}", data))
        return to_db_result(text)

    def change_record_at(self, table_name: str, data: Dict, record_id: str) -> DbResult:
        _, text = self._check(self._simple_request("PUT", f"key/{table_name}/{record_id}", data))
        return to_db_result(text)

    def delete_all_at(self, table_name: str) -> DbResult:
        _, text = self._check(self._simple_request("DELETE", f"key/{table_name}", {}))
        return to_db_result(text)

    def delete_record_at(self, table_name: str, record_id: str) -> DbResult:
        _, text = self._check(self._simple_request("DELETE", f"key/{table_name}/{record_id}", {}))
        return to_db_result(text)

    def patch_all_at(self, table_name: str, data: Dict) -> DbResult:
        _, text = self._check(self._simple_request("PATCH", f"key/{table_name}", data))
        return to_db_result(text)

    def patch_record_at(self, table_name: str, data: Dict, record_id: str) -> DbResult:
        _, text = self._check(self._simple_request("PATCH", f"key/{table_name}/{record_id}", data))
        return to_db_result(text)

    def query(self, query: str) -> DbResult:
        _, text = self._check(self._simple_request("POST", "sql", query, not_json=True))
        return to_db_result(text)

    def import_data(self, path) -> DbResult:
        file = open(path, 'rb')
        _, text = self._check(self._simple_request("POST", "import", file.read().decode(ENCODING), not_json=True))
        return to_db_result(text)

    def _simple_get(self, endpoint: str) -> Tuple[int, str]:
        with self.http_client.get(endpoint) as resp:
            status, text = resp.status, resp.read().decode(ENCODING)
            return status, text

    def _simple_request(self, method, endpoint: str, data: Union[Dict, str], not_json: bool = False) -> Tuple[int, str]:
        with self.http_client.request(method, data, endpoint, not_json) as resp:
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
    print(sur.import_data("test.surql"))
    # print(sur.record_by_id("article", "8bd7423c-921a-4ff1-a6a2-b8bfe098db39"))
    # print(sur.signin("root", "root"))
    # dt = {'created_at': str(datetime.now()),
    #       'author': 'author:john',
    #       'title': 'Lorem ipsum doloqwewqer13',
    #       'text': 'Donec eleifend, nunc vitae commodo accumsan, mauris est fringilla.'}
    # print(sur.new_record_at("article", dt))
    # dt = {'created_at': str(datetime.now()),
    #       'author': 'author:john',
    #       'title': 'New title', }
    # print(sur.change_record_at("article", dt, "8bd7423c-921a-4ff1-a6a2-b8bfe098db39"))
    # dt = {'new_field': 'New title', }
    # print(sur.patch_record_at("article", dt, '8bd7423c-921a-4ff1-a6a2-b8bfe098db39'))
    # print(sur.delete_record_at("article", '8bd7423c-921a-4ff1-a6a2-b8bfe098db39'))
    # print(sur.query("select * from article;"))
