import sys
from pathlib import Path
from unittest import TestCase, main

TESTS = Path(__file__).parent.parent
SRC = TESTS.parent / "src"
sys.path.append(str(SRC))

from surrealist import Surreal, SurrealConnectionError
from surrealist.clients.http_client import mask_opts
from surrealist.result import SurrealResult, to_result

WRONG_URL = "http://127.0.0.1:9999/"
URL = "http://127.0.0.1:8000"


class TestConnections(TestCase):

    def test_raise_on_wrong_url(self):
        db = Surreal(WRONG_URL, 'None', 'None', use_http=True, timeout=1)
        with self.assertRaises(SurrealConnectionError):
            db.connect()

    def test_health(self):
        surreal = Surreal(WRONG_URL, timeout=1)
        with self.assertRaises(SurrealConnectionError):
            surreal.health()

    def test_status(self):
        surreal = Surreal(WRONG_URL, timeout=1)
        with self.assertRaises(SurrealConnectionError):
            surreal.status()

    def test_is_ready(self):
        surreal = Surreal(WRONG_URL, timeout=1)
        with self.assertRaises(SurrealConnectionError):
            surreal.is_ready()

    def test_version(self):
        surreal = Surreal(WRONG_URL, timeout=1)
        with self.assertRaises(SurrealConnectionError):
            surreal.version()

    def test_predicted_url(self):
        surreal = Surreal("wss://127.0.0.1:9000/some/rpc")
        self.assertEqual("https://127.0.0.1:9000/", surreal._possible_url)

    def test_set_url(self):
        surreal = Surreal(URL)
        surreal.set_url("wss://127.0.0.1:9000/some/rpc")
        self.assertEqual("https://127.0.0.1:9000/", surreal._possible_url)
        self.assertEqual("wss://127.0.0.1:9000/some/rpc", surreal._url)


class TestConst(TestCase):

    def test_default_result(self):
        res = SurrealResult()
        self.assertEqual(res.result, None)
        self.assertEqual(res.code, None)
        self.assertEqual(res.status, 'OK')
        self.assertEqual(res.time, None)
        self.assertEqual(res.additional_info, {})

    def test_to_db_result(self):
        res = to_result('[{"result":{}, "status":"ready","time":"0"}]')
        self.assertEqual(SurrealResult(result={}, status="ready", time="0"), res)

    def test_to_auth_result(self):
        res = to_result('{"code":32000, "details":"ready","token":"token"}')
        self.assertEqual(SurrealResult(code=32000, result="ready", token="token", details='ready'), res)

    def test_ws_message_to_result_error(self):
        res = to_result({'id': 100, 'error': {1: 1}})
        self.assertEqual(SurrealResult(id=100, result={1: 1}, status="ERR"), res)
        self.assertTrue(res.is_error())

    def test_ws_message_to_result(self):
        res = to_result({'id': 100, 'result': "result"})
        self.assertEqual(SurrealResult(id=100, result="result"), res)
        self.assertFalse(res.is_error())

    def test_masked_opts(self):
        res = mask_opts({"headers": {"Authorization": "Bearer 123456"}, "method": "GET"})
        self.assertEqual(res, {"headers": {"Authorization": "Bearer ******"}, "method": "GET"})


if __name__ == '__main__':
    main()
