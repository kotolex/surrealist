import json
import threading
import time
import warnings
from json import JSONDecodeError
from threading import Lock
from typing import Dict

import websocket

from py_surreal.errors import WebSocketConnectionClosed
from py_surreal.utils import to_result, SurrealResult, DEFAULT_TIMEOUT, get_uuid


class WebSocketClient:
    def __init__(self, base_url: str, timeout: int = DEFAULT_TIMEOUT):
        # websocket.enableTrace(True)
        self.ws = None
        self.connected = None
        self.timeout = timeout
        self.base_url = base_url
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        self.raise_on_wait(lambda: self.connected is True, timeout=timeout,
                           error_text=f"Not connected to {self.base_url}")
        self.lock = Lock()
        self.dict = {}

    def on_message(self, _ws, message):
        try:
            mess = json.loads(message)
        except JSONDecodeError:
            raise ValueError(f"Got non-json response! {message}")
        if "id" not in mess:
            warnings.warn(f"Got message without id! {mess}")
            mess["id"] = f"unknown_{get_uuid()}"
        id_ = mess["id"]
        self.dict[id_] = mess

    def on_error(self, _ws, err):
        warnings.warn(f"Websocket connection gets an error: {err}")

    def on_open(self, _ws):
        self.connected = True

    def on_close(self, _ws, _close_status_code, _close_msg):
        self.connected = False

    def run(self):
        self.ws = websocket.WebSocketApp(self.base_url, on_open=self.on_open, on_message=self.on_message,
                                         on_error=self.on_error, on_close=self.on_close)
        self.ws.run_forever(ping_interval=self.timeout)

    def send(self, data: Dict) -> SurrealResult:
        id_ = get_uuid()
        data = {"id": id_, **data}
        self.ws.send(json.dumps(data))
        res = self._get_by_id(id_)
        return to_result(res)

    def _get_by_id(self, id_) -> Dict:
        self.raise_on_wait(lambda: id_ in self.dict, timeout=self.timeout,
                           error_text=f"No messages with id {id_} received in {self.timeout} seconds")
        with self.lock:
            result = self.dict.pop(id_, None)
        if result is None:
            # Should never happen!
            raise ValueError(f"Dict returns None on thread-safe pop, {id_=}, {self.dict=}")
        return result

    def _wait_until(self, predicate, timeout, period=0.25):
        mustend = time.time() + timeout
        while time.time() < mustend:
            if self.connected is False:
                return False, "CLOSED"
            if predicate():
                return True, None
            time.sleep(period)
        return False, "TIME"

    def raise_on_wait(self, predicate, timeout, error_text):
        result = self._wait_until(predicate, timeout)
        if result == (False, "TIME"):
            raise TimeoutError(f"Time exceeded: {timeout} seconds. Error: {error_text}")
        elif result == (False, "CLOSED"):
            raise WebSocketConnectionClosed("Connection closed")

    def close(self):
        self.connected = False
        self.ws.close()
        del self.ws
        self.dict.clear()
