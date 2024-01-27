from unittest import TestCase, main
from py_surreal.surreal import Surreal
from py_surreal.errors import SurrealConnectionError
from py_surreal.utils import (to_auth_result, to_db_result, ws_message_to_result, DbResult, AuthResult, DbError,
                              DbSimpleResult)


class TestConnections(TestCase):

    def test_raise_on_wrong_url(self):
        db = Surreal("http://127.0.0.1:9999/", 'None', 'None', use_http=True, timeout=1)
        with self.assertRaises(SurrealConnectionError):
            db.connect()


class TestConst(TestCase):

    def test_to_db_result(self):
        res = to_db_result('[{"result":{}, "status":"ready","time":"0"}]')
        self.assertEqual(DbResult({}, "ready", "0"), res)

    def test_to_auth_result(self):
        res = to_auth_result('{"code":32000, "details":"ready","token":"token"}')
        self.assertEqual(AuthResult(32000, "ready", "token"), res)

    def test_ws_message_to_result_error(self):
        res = ws_message_to_result({'id': 100, 'error': {1: 1}})
        self.assertEqual(DbError(100, {1: 1}), res)
        self.assertTrue(res.is_error())

    def test_ws_message_to_result(self):
        res = ws_message_to_result({'id': 100, 'result': "result"})
        self.assertEqual(DbSimpleResult(100, "result"), res)
        self.assertFalse(res.is_error())


if __name__ == '__main__':
    main()
