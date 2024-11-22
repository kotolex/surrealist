from unittest import TestCase, main

from tests.integration_tests.utils import URL
from surrealist import (Surreal, SurrealConnectionError, WebSocketConnectionError, CompatibilityError,
                        DatabaseConnectionsPool, WrongParameterError)


class TestNegativeWebSocketConnection(TestCase):
    def test_connect_failed_on_invalid_url(self):
        surreal = Surreal("http://127.0.0.1:8001", credentials=('root', 'root'), timeout=1)
        with self.assertRaises(SurrealConnectionError):
            surreal.connect()

    def test_connect_failed_on_invalid_url_pool(self):
        with self.assertRaises(SurrealConnectionError):
            DatabaseConnectionsPool("http://127.0.0.1:8001", 'test', 'test', credentials=('user_db', 'user_db'),
                                    timeout=1)

    def test_connect_failed_on_wrong_creds(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('wrong', 'wrong'))
        with self.assertRaises(WebSocketConnectionError):
            surreal.connect()

    def test_connect_failed_on_wrong_creds_no_ns(self):
        surreal = Surreal(URL, credentials=('wrong', 'wrong'))
        with self.assertRaises(WebSocketConnectionError):
            surreal.connect()

    def test_live_failed_on_id(self):
        a_list = []
        function = lambda mess: a_list.append(mess)
        surreal = Surreal(URL, "test", "test", credentials=('user_db', 'user_db'),)
        with surreal.connect() as connection:
            res = connection.live(f"ws_article:some_id", callback=function)
            self.assertTrue(res.is_error(), res)
            self.assertEqual(res.result, "Can not execute LIVE statement using value 'ws_article:some_id'")
            self.assertEqual(res.code, -32000)

    def test_kill(self):
        surreal = Surreal(URL, "test", "test", credentials=('user_db', 'user_db'),)
        with surreal.connect() as connection:
            res = connection.kill("wrong")
            self.assertTrue(res.is_error(), res)
            self.assertEqual(res.result, "Can not execute KILL statement using id ''wrong''")
            self.assertEqual(res.code, -32000)
            res = connection.kill("0189d6e3-8eac-703a-9a48-d9faa78b44b9")
            self.assertTrue(res.is_error(), res)
            self.assertEqual(res.result, "Can not execute KILL statement using id ''0189d6e3-8eac-703a-9a48-d9faa78b44b9''")
            self.assertEqual(res.code, -32000)

    def test_export_failed(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.export()

    def test_ml_export_failed(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.ml_export(None, None)

    def test_import_failed(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.import_data(None)

    def test_ml_import_failed(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.ml_import(None)

    def test_db_info_failed_permissions(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            res = connection.db_info()
            self.assertTrue(res.is_error())
            self.assertEqual("INFO FOR DB;", res.query)
            self.assertEqual("Specify a namespace to use", res.result)

    def test_insert_failed_if_have_id(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'),)
        with surreal.connect() as connection:
            res = connection.insert("new_table:new_insert", {'new_field2': 'field2'})
            self.assertTrue(res.is_error())

    def test_root_info_failed(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'),)
        with surreal.connect() as connection:
            res = connection.root_info()
            self.assertTrue(res.is_error())
            self.assertEqual("IAM error: Not enough permissions to perform this action", res.result)

    def test_ns_info_failed(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.ns_info()
            self.assertTrue(res.is_error())
            self.assertEqual("IAM error: Not enough permissions to perform this action", res.result)

    def test_run_3_args(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            connection.use("test", "test")
            res = connection.run("ml::image_classifier", "v2.1", ["image_data_base64"])
            self.assertTrue(res.is_error(), res)
            self.assertEqual("The model 'ml::image_classifier<v2.1>' does not exist", res.result)

    def test_graphql_invalid(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            connection.use("test", "test")
            with self.assertRaises(WrongParameterError):
                connection.graphql({"wrong": "{ author { id } }"}, pretty=True)


if __name__ == '__main__':
    main()
