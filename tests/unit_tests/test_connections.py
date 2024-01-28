from unittest import TestCase, main
from py_surreal.surreal import Surreal
from py_surreal.errors import SurrealConnectionError
from py_surreal.utils import to_result, SurrealResult


class TestConnections(TestCase):

    def test_raise_on_wrong_url(self):
        db = Surreal("http://127.0.0.1:9999/", 'None', 'None', use_http=True, timeout=1)
        with self.assertRaises(SurrealConnectionError):
            db.connect()


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


if __name__ == '__main__':
    main()
