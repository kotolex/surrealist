import json
import threading
import time
import urllib.parse
from typing import Optional, Tuple, Dict
import websocket
from collections import deque

from py_surreal.const import DbError, ws_message_to_result, Result


def wait_until(predicate, timeout, period=0.25):
    mustend = time.time() + timeout
    while time.time() < mustend:
        if predicate():
            return True
        time.sleep(period)
    return False


def raise_on_wait(predicate, timeout):
    if not wait_until(predicate, timeout):
        raise ValueError(f"Not connected during timeout {timeout} seconds")


class WebSocketClient:
    def __init__(self, base_url: str, headers: Optional[Dict] = None, credentials: Optional[Tuple[str, str]] = None,
                 timeout: int = 5):
        # websocket.enableTrace(True)
        base_url = urllib.parse.urlparse(base_url.lower())
        self.connected = False
        self.timeout = timeout
        if base_url.scheme in ("ws", "wss"):
            self._base_url = base_url
        if base_url.scheme in ("http", "https"):
            self._base_url = f"{base_url.scheme.replace('http', 'ws')}://{base_url.netloc}/rpc"
        self.credentials = credentials
        self._user, self._pass = (None, None)
        self.namespace = None
        self.database = None
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        raise_on_wait(lambda: self.connected is True, timeout=timeout)
        self.queue = deque()
        self.num = 1
        if headers:
            self.namespace = headers.get("NS")
            self.database = headers.get("DB")
            if credentials:
                self._user, self._pass = self.credentials
                signin_result = self.signin(self._user, self._pass, (self.namespace, self.database))
                if signin_result.is_error():
                    raise ValueError(f"Error on connecting to '{self._base_url}'. \nInfo: {signin_result}")
                print(signin_result)
            else:
                use_result = self.use(self.namespace, self.database)
                if use_result.is_error():
                    raise ValueError(f"Error on use '{(self.namespace, self.database)}'. \nInfo: {use_result}")
                print(use_result)
        else:
            if credentials:
                self._user, self._pass = self.credentials
                signin_result = self.signin(self._user, self._pass)
                if signin_result.is_error():
                    raise ValueError(f"Error on connecting to '{self._base_url}'. \nInfo: {signin_result}")
                print(signin_result)

    def on_message(self, _wsapp, message):
        print(message)
        self.queue.append(message)

    def on_error(self, ws, err):
        print(err)

    def on_open(self, _ws):
        self.connected = True

    def on_close(self, _ws, close_status_code, close_msg):
        print(close_status_code, close_msg)
        print(">>>>>>CLOSED")

    def run(self):
        self.ws = websocket.WebSocketApp(self._base_url, on_open=self.on_open, on_message=self.on_message,
                                         on_error=self.on_error, on_close=self.on_close)
        self.ws.run_forever(ping_interval=5)

    def use(self, ns, db):
        self.namespace = ns
        self.database = db
        data = {"id": self.num, "method": "use", "params": [ns, db]}
        return self._run(data)

    def info(self):
        data = {"id": self.num,  "method": "info"}
        return self._run(data)

    def signin(self, user: str, password: str, ns_db: Optional[Tuple[str, str]] = None):
        data = {"id": self.num, "method": "signin", "params": [{"user": user, "pass": password}]}
        if ns_db:
            ns, db = ns_db
            data['params'][0] = {"NS": ns, "DB": db, **data['params'][0]}
        return self._run(data)

    def all_records_at(self, name: str):
        data = {"id": self.num, "method": "select", "params": [name]}
        return self._run(data)

    def _run(self, data) -> Result:
        self.ws.send(json.dumps(data))
        res = self._get_by_id(self.num)
        self.num += 1
        return ws_message_to_result(res)

    def _get_by_id(self, id_):
        raise_on_wait(lambda: len(self.queue) > 0, timeout=self.timeout)
        a_list = [json.loads(e) for e in list(self.queue)]
        self.queue.clear()
        found = [e for e in a_list if e["id"] == id_]
        if not found:
            raise ValueError(f"Not found id {id_}, queue: {a_list}")
        for e in a_list:
            if e["id"] != id_:
                self.queue.append(e)
        return found[0]


if __name__ == '__main__':
    cl = WebSocketClient("http://127.0.0.1:8000/", {"NS": "test", "DB": "test"}, credentials=('root', 'root'))
    # cl = WebSocketClient("http://127.0.0.1:8000/", {"NS": "test", "DB": "test"})
    print(cl.connected)
    # cl.use("test", "test")
    time.sleep(3)
    print(cl.info())
    print(cl.all_records_at("article"))
    time.sleep(3)
