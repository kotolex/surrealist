from unittest import TestCase, main
from py_surreal.surreal import Surreal
from py_surreal.errors import SurrealConnectionError, HttpClientError
from py_surreal.utils import to_result, SurrealResult, crop_data, mask_pass
from py_surreal.clients.http_client import mask_opts

WRONG_URL = "http://127.0.0.1:9999/"
URL = "http://127.0.0.1:8000"


class TestConnections(TestCase):

    def test_raise_on_wrong_url(self):
        db = Surreal(WRONG_URL, 'None', 'None', use_http=True, timeout=1)
        with self.assertRaises(SurrealConnectionError):
            db.connect()

    def test_raise_on_wrong_level(self):
        with self.assertRaises(ValueError):
            Surreal(URL, log_level="WARN")

    def test_health(self):
        surreal = Surreal(WRONG_URL)
        with self.assertRaises(HttpClientError):
            surreal.health()

    def test_status(self):
        surreal = Surreal(WRONG_URL)
        with self.assertRaises(HttpClientError):
            surreal.status()

    def test_is_ready(self):
        surreal = Surreal(WRONG_URL)
        with self.assertRaises(HttpClientError):
            surreal.is_ready()

    def test_version(self):
        surreal = Surreal(WRONG_URL)
        with self.assertRaises(HttpClientError):
            surreal.version()

    def test_predicted_url(self):
        surreal = Surreal("wss://127.0.0.1:9000/some/rpc")
        self.assertEqual("https://127.0.0.1:9000/", surreal._possible_url)


class TestConst(TestCase):

    def test_default_result(self):
        res = SurrealResult()
        self.assertEqual(res.result, None)
        self.assertEqual(res.code, None)
        self.assertEqual(res.token, None)
        self.assertEqual(res.status, 'OK')
        self.assertEqual(res.time, '')

    def test_to_db_result(self):
        res = to_result('[{"result":{}, "status":"ready","time":"0"}]')
        self.assertEqual(SurrealResult(result={}, status="ready", time="0"), res)

    def test_to_auth_result(self):
        res = to_result('{"code":32000, "details":"ready","token":"token"}')
        self.assertEqual(SurrealResult(code=32000, result="ready", token="token"), res)

    def test_ws_message_to_result_error(self):
        res = to_result({'id': 100, 'error': {1: 1}})
        self.assertEqual(SurrealResult(id=100, error={1: 1}), res)
        self.assertTrue(res.is_error())

    def test_ws_message_to_result(self):
        res = to_result({'id': 100, 'result': "result"})
        self.assertEqual(SurrealResult(id=100, result="result"), res)
        self.assertFalse(res.is_error())

    def test_crop_same(self):
        self.assertEqual(crop_data("one"), "one")

    def test_crop_data(self):
        self.assertEqual(crop_data("*" * 400), "*" * 300 + "...")

    def test_mask_pass(self):
        self.assertEqual(mask_pass(
            "{'method': 'signin', 'params': [{'user': 'root', 'pass': '******', 'NS': 'test', 'DB': 'test'}]}"),
            "{'method': 'signin', 'params': [{'user': 'root', 'pass': '******', 'NS': 'test', 'DB': 'test'}]}")
        self.assertEqual(mask_pass('{"user":"user", "pass": "123123"}'), '{"user":"user", "pass": "******"}')

    def test_masked_opts(self):
        res = mask_opts({"headers": {"Authorization": "Basic 123456"}, "data": b'0' * 350, "method": "GET"})
        self.assertEqual(res,
                         {"headers": {"Authorization": "Basic ******"}, "data": "b'" + "0" * 298 + "...",
                          "method": "GET"})


if __name__ == '__main__':
    main()
