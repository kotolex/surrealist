import json
import threading
import time
import warnings
from typing import Dict

import websocket
from collections import deque

from py_surreal.errors import WebSocketConnectionClosed
from py_surreal.utils import to_result, SurrealResult, DEFAULT_TIMEOUT


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
        self.queue = deque()
        self.num = 1

    def on_message(self, _ws, message):
        self.queue.append(message)

    def on_error(self, _ws, err):
        warnings.warn(f"Websocket connection gets an error:{err}")

    def on_open(self, _ws):
        self.connected = True

    def on_close(self, _ws, _close_status_code, _close_msg):
        self.connected = False

    def run(self):
        self.ws = websocket.WebSocketApp(self.base_url, on_open=self.on_open, on_message=self.on_message,
                                         on_error=self.on_error, on_close=self.on_close)
        self.ws.run_forever(ping_interval=5)

    def send(self, data: Dict) -> SurrealResult:
        data = {"id": self.num, **data}
        return self._run(data)

    def _run(self, data) -> SurrealResult:
        self.ws.send(json.dumps(data))
        res = self._get_by_id(self.num)
        self.num += 1
        return to_result(res)

    def _get_by_id(self, id_):
        self.raise_on_wait(lambda: len(self.queue) > 0, timeout=self.timeout, error_text="No messages received")
        a_list = [json.loads(e) for e in list(self.queue)]
        self.queue.clear()
        found = [e for e in a_list if e["id"] == id_]
        if not found:
            raise ValueError(f"Not found id {id_}, queue: {a_list}")
        for e in a_list:
            if e["id"] != id_:
                self.queue.append(e)
        return found[0]

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
            raise WebSocketConnectionClosed(f"Connection closed")

    def close(self):
        self.connected = False
        self.ws.close()
        del self.ws
        self.queue.clear()
