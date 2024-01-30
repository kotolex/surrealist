import json
import threading
import time
import warnings
from json import JSONDecodeError
from threading import Lock
from typing import Dict, Callable, Optional

import websocket

from py_surreal.errors import WebSocketConnectionClosedError
from py_surreal.utils import to_result, SurrealResult, DEFAULT_TIMEOUT, get_uuid


class WebSocketClient:
    """
    Blocking thread-safe client to work with websockets, always wait a response for message, comparing by id (uuid).
    Every client creates at least 2 threads (in and out)
    """

    def __init__(self, base_url: str, timeout: int = DEFAULT_TIMEOUT):
        self.ws = None
        self.connected = None
        self.timeout = timeout
        self.base_url = base_url
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        self._raise_on_wait(lambda: self.connected is True, timeout=timeout,
                            error_text=f"Not connected to {self.base_url}")
        self.lock = Lock()
        self.callbacks = {}
        self.messages = {}

    def on_message(self, _ws, message):
        try:
            mess = json.loads(message)
        except JSONDecodeError:
            # Should never happen, all messages via json
            raise ValueError(f"Got non-json response! {message}")
        if "id" in mess:
            id_ = mess["id"]
            self.messages[id_] = mess
        else:
            # no id at top level = live query received
            if 'result' in mess:
                live_id = mess['result']['id']
                callback = self.callbacks.get(live_id)
                if callback:
                    callback(mess)
            else:
                warnings.warn(f"Got unexpected message without id and result: {mess}")

    def on_error(self, _ws, err):
        # print(type(err))
        # print(f"_{err}_")
        warnings.warn(f"Websocket connection gets an error: {err}")

    def on_open(self, _ws):
        self.connected = True

    def on_close(self, *_ignore):
        self.connected = False

    def run(self):
        self.ws = websocket.WebSocketApp(self.base_url, on_open=self.on_open, on_message=self.on_message,
                                         on_error=self.on_error, on_close=self.on_close)
        self.ws.run_forever(ping_interval=self.timeout, skip_utf8_validation=True)

    def send(self, data: Dict, callback: Optional[Callable] = None) -> SurrealResult:
        """
        Method to send messages to SurrealDB, blocks until gets a response or raise on timeout

        :param data: dict with request parameters
        :param callback: function to call on live query, it is set only for live method
        :return: result of the request
        :raise TimeoutError if no response and time is over
        :raise WebSocketConnectionClosed if connection was closed while waiting
        """
        id_ = get_uuid()
        data = {"id": id_, **data}
        self.ws.send(json.dumps(data))
        res = self._get_by_id(id_)
        if data['method'] in ('live', 'kill'):
            if 'error' not in res:
                # now we know live or kill was successful, so now we need to manage callbacks
                self._on_success(data, callback, res)
        return to_result(res)

    def _on_success(self, data: Dict, callback: Callable, result: Dict):
        if data['method'] == 'kill':
            self.callbacks[data['params'][0]] = None
        if data['method'] == 'live':
            self.callbacks[result['result']] = callback

    def _get_by_id(self, id_) -> Dict:
        self._raise_on_wait(lambda: id_ in self.messages, timeout=self.timeout,
                            error_text=f"No messages with id {id_} received in {self.timeout} seconds")
        with self.lock:
            result = self.messages.pop(id_, None)
        if result is None:
            # Should never happen!
            raise ValueError(f"Dict returns None on thread-safe pop, {id_=}, {self.messages=}")
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

    def _raise_on_wait(self, predicate, timeout, error_text):
        """
        Method to wait some condition or raise on timeout


        :param predicate: function to call and check condition
        :param timeout: time in seconds to wait until condition
        :param error_text: custom error message on fail
        :raise TimeoutError if condition not met until time
        :raise WebSocketConnectionClosed if connection was closed while waiting
        """
        result = self._wait_until(predicate, timeout)
        if result == (False, "TIME"):
            raise TimeoutError(f"Time exceeded: {timeout} seconds. Error: {error_text}")
        elif result == (False, "CLOSED"):
            raise WebSocketConnectionClosedError("Connection closed")

    def close(self):
        self.connected = False
        self.ws.close()
        del self.ws
        self.messages.clear()
        self.callbacks.clear()
