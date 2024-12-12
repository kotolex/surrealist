import json
import threading
import time
from json import JSONDecodeError
from logging import getLogger
from queue import Empty, Queue
from typing import Callable, Dict, Optional

import websocket

from surrealist.errors import (TooManyNestedLevelsError,
                               WebSocketConnectionClosedError)
from surrealist.result import SurrealResult, to_result
from surrealist.utils import DEFAULT_TIMEOUT, get_uuid, mask_pass

logger = getLogger("surrealist.clients.websocket")


class WebSocketClient:
    """
    Synchronous thread-safe client to work with websockets, always wait a response for message, comparing by id (uuid).
    Every client creates at least two threads (in and out)
    """

    def __init__(self, base_url: str, timeout: int = DEFAULT_TIMEOUT):
        self._ws = None
        self._connected = None
        self._timeout = timeout
        self._base_url = base_url
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        logger.debug("Connecting to %s", base_url)
        self._raise_on_wait(lambda: self._connected is True, timeout=timeout,
                            error_text=f"Not connected to {self._base_url}")
        self._callbacks = {}
        self._messages: Dict[str, Queue] = {}
        logger.debug("Connected to %s, timeout is %s seconds", base_url, timeout)

    def on_message(self, _ws, message: str):
        """
        Called on a message received from the websocket connection.

        :param _ws: connection object
        :param message: string message
        """
        logger.debug("Get message %s", message)
        try:
            mess = json.loads(message)
        except JSONDecodeError as je:
            # Should never happen, all messages via json
            logger.error("Got non-json response %s", message, exc_info=True)
            raise ValueError(f"Got non-json response! {message}") from je
        except RecursionError as e:
            logger.error("Cant deserialize object, too many nested levels")
            raise TooManyNestedLevelsError("Cant serialize object, too many nested levels") from e
        if "id" in mess:
            id_ = mess["id"]
            self._messages[id_].put_nowait(mess)
        else:
            # no id at top level = live query received
            if 'result' in mess:
                live_id = mess['result']['id']
                callback = self._callbacks.get(live_id)
                if callback:
                    logger.debug("Use callback for %s", live_id)
                    callback(mess)
                else:
                    logger.warning("Got a message, but no callback to work with. Message: %s", mess)
            else:
                logger.warning("Got an unexpected message without id and result: %s", mess)

    def on_error(self, _ws, err: Exception):
        """
        Callback on getting any errors with sockets
        """
        logger.error("Websocket connection gets an error %s", err)

    def is_connected(self) -> bool:
        """
        Shows is a websocket client is connected to SurrealDB

        :return: True if connected, False otherwise
        """
        return bool(self._connected)

    def on_open(self, _ws):
        """
        Callback on establishing new connection
        """
        self._connected = True

    def on_close(self, *_ignore):
        """
        Callback on closing websocket connection
        """
        self._connected = False
        logger.debug("Close connection to %s", self._base_url)

    def run(self):
        """
        Constantly waiting for messages on websocket connection, runs in separate thread

        :return: None
        """
        self._ws = websocket.WebSocketApp(self._base_url, header={'sec-websocket-protocol': 'json'},
                                          on_open=self.on_open, on_message=self.on_message,
                                          on_error=self.on_error, on_close=self.on_close)
        self._ws.run_forever(skip_utf8_validation=True)  # works faster

    def send(self, data: Dict, callback: Optional[Callable] = None) -> SurrealResult:
        """
        Method to send messages to SurrealDB, blocks until gets a response or raise on timeout

        :param data: dict with request parameters
        :param callback: function to call on a live query, it is set only for a live method
        :return: result of the request
        :raise TimeoutError: if no response and time is over
        :raise WebSocketConnectionClosed: if the connection was closed while waiting
        """
        id_ = get_uuid()
        data = {"id": id_, **data}
        to_send = data if "additional" not in data else {k: v for k, v in data.items() if k != "additional"}
        try:
            data_string = json.dumps(to_send, ensure_ascii=False)
        except RecursionError as e:
            logger.error("Cant serialize object, too many nested levels")
            raise TooManyNestedLevelsError("Cant serialize object, too many nested levels") from e
        logger.debug("Send data: %s", mask_pass(data_string))
        self._messages[id_] = Queue(maxsize=1)
        self._ws.send(data_string)
        res = self._get_by_id(id_)
        if data['method'] in ('live', 'kill') or "additional" in data:
            if 'error' not in res:
                # now we know live or kill was successful, so now we need to manage callbacks
                self._on_success(data, callback, res)
        return to_result(res)

    def _on_success(self, data: Dict, callback: Callable, result: Dict):
        if data['method'] == 'kill':
            logger.debug("Delete callback for %s", data['params'][0])
            self._callbacks[data['params'][0]] = None
        else:
            # custom query returns nested result
            key = result['result'] if data['method'] == 'live' else result['result'][0]['result']
            logger.debug("Set callback for %s", result['result'])
            self._callbacks[key] = callback

    def _get_by_id(self, id_) -> Dict:
        try:
            result = self._messages[id_].get(timeout=self._timeout)
        except Empty as exc:
            raise TimeoutError(f"Time exceeded: {self._timeout} seconds, no response received") from exc
        finally:
            del self._messages[id_]
        return result

    def _wait_until(self, predicate, timeout, period=0.0005):
        must_end = time.time() + timeout
        while time.time() < must_end:
            if self._connected is False:
                return False, "CLOSED"
            if predicate():
                return True, None
            time.sleep(period)
        return False, "TIME"

    def _raise_on_wait(self, predicate, timeout, error_text):
        """
        Method to wait some condition or raise on timeout

        :param predicate: function to call and check condition
        :param timeout: time in seconds to wait until a condition
        :param error_text: custom error message on fail
        :raise TimeoutError: if condition not met until time
        :raise WebSocketConnectionClosed: if connection was closed while waiting
        """
        result = self._wait_until(predicate, timeout)
        if result == (False, "TIME"):
            logger.error("Time exceeded: %s seconds. Error: %s", timeout, error_text)
            raise TimeoutError(f"Time exceeded: {timeout} seconds. Error: {error_text}")
        if result == (False, "CLOSED"):
            logger.error("Connection %s closed while a client waits on it", self._base_url)
            raise WebSocketConnectionClosedError("Connection closed while a client waits on it")

    def close(self):
        """
        Close websocket client and close websocket connection, you cannot use this object after close
        """
        self._connected = False
        self._ws.close()
        del self._ws
        self._messages.clear()
        self._callbacks.clear()
        logger.debug("Client is closed connection to %s", self._base_url)
