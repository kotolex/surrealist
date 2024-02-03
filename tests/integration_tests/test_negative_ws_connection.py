from unittest import TestCase, main

from tests.integration_tests.utils import URL
from surrealist import Surreal, SurrealConnectionError, WebSocketConnectionError, CompatibilityError


class TestNegativeWebSocketConnection(TestCase):
    def test_connect_failed_on_invalid_url(self):
        surreal = Surreal("http://127.0.0.1:8001", namespace="test", database="test", credentials=('root', 'root'),
                          timeout=1)
        with self.assertRaises(SurrealConnectionError):
            surreal.connect()

    def test_connect_failed_on_wrong_creds(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('wrong', 'wrong'))
        with self.assertRaises(WebSocketConnectionError):
            surreal.connect()

    def test_connect_failed_on_wrong_creds_no_ns(self):
        surreal = Surreal(URL, credentials=('wrong', 'wrong'))
        with self.assertRaises(WebSocketConnectionError):
            surreal.connect()

    def test_connect_failed_no_db(self):
        surreal = Surreal(URL, namespace="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            res = connection.info()
            self.assertTrue(res.is_error(), res)
            self.assertTrue("Specify a database to use" in res.error["message"])

    def test_info_failed_no_ns(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            res = connection.info()
            self.assertTrue(res.is_error(), res)
            self.assertTrue("Specify a namespace to use" in res.error["message"])

    def test_authenticate_failed_wrong_token(self):
        surreal = Surreal(URL, namespace="test", database="test")
        with surreal.connect() as connection:
            res = connection.authenticate("wrong")
            self.assertTrue(res.is_error(), res)
            self.assertEqual(res.error['message'], 'There was a problem with authentication')
            self.assertEqual(res.error['code'], -32000)

    def test_signin_failed_root(self):
        surreal = Surreal(URL, namespace="test", database="test")
        with surreal.connect() as connection:
            res = connection.signin('wrong', 'wrong')
            self.assertTrue(res.is_error(), res)
            self.assertEqual(res.error['message'], 'There was a problem with authentication')
            self.assertEqual(res.error['code'], -32000)

    def test_live_failed_on_id(self):
        a_list = []
        function = lambda mess: a_list.append(mess)
        surreal = Surreal(URL, namespace="test", database="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            res = connection.live(f"ws_article:some_id", callback=function)
            self.assertTrue(res.is_error(), res)
            self.assertEqual({'code': -32602, 'message': 'Invalid params'}, res.error)

    def test_kill(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            res = connection.kill("wrong")
            self.assertTrue(res.is_error(), res)
            self.assertEqual(res.error['message'],
                             "There was a problem with the database: Can not execute KILL statement using id '$id'")
            self.assertEqual(res.error['code'], -32000)

    def test_export_failed(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.export()

    def test_ml_export_failed(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.ml_export(None, None)

    def test_import_failed(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.import_data(None)

    def test_ml_import_failed(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.ml_import(None)

    def test_db_info_failed_permissions(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            res = connection.db_info()
            self.assertTrue(res.is_error())
            self.assertEqual("INFO FOR DB;", res.query)
            self.assertEqual("Specify a namespace to use", res.error)


if __name__ == '__main__':
    main()
