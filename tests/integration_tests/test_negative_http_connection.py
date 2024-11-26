from unittest import TestCase, main

from tests.integration_tests.utils import URL
from surrealist import (HttpConnectionError, SurrealConnectionError, CompatibilityError, SurrealRecordIdError)
from surrealist import Surreal, get_uuid


PARAMS = (
    ('Specify a namespace to use', {'credentials': ('root', 'root'), }),
    ('Specify a database to use', {'namespace': 'test', 'credentials': ('user_ns', 'user_ns'), }),
)


class TestHttpConnectionNegative(TestCase):

    def test_create_one_failed_on_2_id(self):
        db = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with db.connect() as connection:
            connection.use("test", "test")
            uid = get_uuid()
            uid2 = get_uuid()
            with self.assertRaises(SurrealRecordIdError):
                connection.create(f"article:`{uid}`", {"author": uid, "title": uid, "text": uid}, record_id=uid2)

    def test_create_many_failed(self):
        db = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with db.connect() as connection:
            connection.use("test", "test")
            uid = get_uuid()
            uid2 = get_uuid()
            res = connection.create("article", [{"id": uid, "author": uid, "title": uid, "text": uid},
                                                {"id": uid2, "author": uid2, "title": uid2, "text": uid2}])
            self.assertEqual(res.status, "ERR", res)

    def test_query_failed(self):
        db = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with db.connect() as connection:
            connection.use("test", "test")
            res = connection.query("SELECT * FROM DATA NOT REALLY AN SQL;")
            self.assertTrue(res.is_error())
            self.assertTrue("Unexpected token" in res.result, res)

    def test_ml_export_failed(self):
        db = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with db.connect() as connection:
            connection.use("test", "test")
            with self.assertRaises(HttpConnectionError):
                connection.ml_export("prediction", "1.0.0")

    def test_select_one_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"select failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    res = connection.select("article", "any")
                    self.assertTrue(res.is_error())
                    self.assertTrue(expected in res.result, res)

    def test_select_all_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"select all failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    res = connection.select("article")
                    self.assertTrue(res.is_error())
                    self.assertTrue(expected in res.result, res)

    def test_create_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"create failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    res = connection.create("article", {}, "any")
                    self.assertTrue(res.is_error())
                    self.assertTrue(expected in res.result, res)

    def test_update_one_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"update one failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    res = connection.update("article", {}, "any")
                    self.assertTrue(res.is_error())
                    self.assertTrue(expected in res.result, res)

    def test_update_all_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"update all failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    res = connection.update("article", {})
                    self.assertTrue(res.is_error())
                    self.assertTrue(expected in res.result, res)

    def test_merge_one_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"merge one failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    res = connection.merge("article", {}, "any")
                    self.assertTrue(res.is_error())
                    self.assertTrue(expected in res.result, res)

    def test_merge_all_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"merge all failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    res = connection.merge("article", {})
                    self.assertTrue(res.is_error())
                    self.assertTrue(expected in res.result, res)

    def test_delete_one_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"delete one failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    res = connection.delete("article", "any")
                    self.assertTrue(res.is_error())
                    self.assertTrue(expected in res.result, res)

    def test_live_failed(self):
        db = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with db.connect() as connection:
            connection.use("test", "test")
            with self.assertRaises(CompatibilityError):
                connection.live("prediction", print)

    def test_custom_live_failed(self):
        db = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with db.connect() as connection:
            connection.use("test", "test")
            with self.assertRaises(CompatibilityError):
                connection.custom_live("LIVE SELECT FROM person;", print)

    def test_kill_failed(self):
        db = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with db.connect() as connection:
            connection.use("test", "test")
            with self.assertRaises(CompatibilityError):
                connection.kill("prediction")

    def test_delete_all_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"delete all failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    res = connection.delete("article")
                    self.assertTrue(res.is_error())
                    self.assertTrue(expected in res.result, res)

    def test_connect_failed(self):
        params = (
            ('Cant sign', {'credentials': ('wrong', 'wrong')}),
            ('Cant sign', {'credentials': ('root', 'wrong')}),
        )
        for expected, opts in params:
            with self.subTest(f"connect failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with self.assertRaises(SurrealConnectionError) as e:
                    db.connect()
                self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    # def test_ml_import_failed(self):  # TODO uncomment on fix https://github.com/surrealdb/surrealdb/issues/4236
    #     for expected, opts in PARAMS:
    #         with self.subTest(f"ml import failed on data{opts}"):
    #             db = Surreal(URL, use_http=True, **opts)
    #             file_path = Path(__file__).parent / "import.surql"
    #             with db.connect() as connection:
    #                 res = connection.ml_import(file_path)
    #                 self.assertTrue(res.is_error())
    #                 self.assertTrue(expected in res.result, res)

    def test_export_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"export failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.export()
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    # def test_import_empty(self):  # TODO uncomment on fix https://github.com/surrealdb/surrealdb/issues/4263
    #     params = (
    #         ("Specify some SQL code to execute", "empty.surql"),
    #         ("Unexpected token", "wrong.surql")
    #     )
    #     for expected, file in params:
    #         with self.subTest(f"Import {file}"):
    #             file_path = Path(__file__).parent / file
    #             db = Surreal(URL, credentials=('root', 'root'), use_http=True)
    #             with db.connect() as connection:
    #                 res = connection.import_data(file_path)
    #                 self.assertTrue(res.is_error(), res)
    #                 self.assertTrue(expected in res.result, res)

    def test_db_info_failed_permissions(self):
        surreal = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with surreal.connect() as connection:
            res = connection.db_info()
            self.assertTrue(res.is_error())
            self.assertEqual("INFO FOR DB;", res.query)
            self.assertEqual("Specify a namespace to use", res.result)

    def test_insert_failed_if_have_id(self):
        surreal = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with surreal.connect() as connection:
            res = connection.insert("new_table:new_insert", {'new_field2': 'field2'})
            self.assertTrue(res.is_error())
            self.assertEqual(-32000, res.code)

    def test_root_info_failed(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'), use_http=True)
        with surreal.connect() as connection:
            res = connection.root_info()
            self.assertTrue(res.is_error())
            self.assertEqual("IAM error: Not enough permissions to perform this action", res.result)

    def test_ns_info_failed(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'), use_http=True)
        with surreal.connect() as connection:
            res = connection.ns_info()
            self.assertTrue(res.is_error())
            self.assertEqual("IAM error: Not enough permissions to perform this action", res.result)

    def test_run_3_args(self):
        surreal = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with surreal.connect() as connection:
            connection.use("test", "test")
            res = connection.run("ml::image_classifier", "v2.1", ["image_data_base64"])
            self.assertTrue(res.is_error(), res)
            self.assertEqual("The model 'ml::image_classifier<v2.1>' does not exist", res.result)

    # TODO uncomment after bugfix
    # def test_ml_import_failed_wrong_file(self):
    #     db = Surreal(URL, credentials=('root', 'root'), use_http=True)
    #     file_path = Path(__file__).parent / "import.srql"
    #     with db.connect() as connection:
    #     with self.assertRaises(HttpClientError):
    #         connection.ml_import(file_path)


if __name__ == '__main__':
    main()
