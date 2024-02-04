from pathlib import Path
from unittest import TestCase, main

from tests.integration_tests.utils import URL
from surrealist.utils import get_uuid
from surrealist import HttpConnectionError, SurrealConnectionError, CompatibilityError
from surrealist import Surreal

PARAMS = (
    ('Specify a namespace to use', {'credentials': ('root', 'root'), }),
    ('Specify a namespace to use', {'database': 'test', 'credentials': ('root', 'root'), }),
    ('Specify a database to use', {'namespace': 'test', 'credentials': ('root', 'root'), }),
    ('Not enough permissions to perform this action', {'namespace': 'test', 'database': 'test'}),
)


class TestHttpConnectionNegative(TestCase):

    def test_create_one_failed_on_2_id(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            uid = get_uuid()
            uid2 = get_uuid()
            res = connection.create("article", {"id": uid, "author": uid, "title": uid, "text": uid}, record_id=uid2)
            self.assertEqual(res.status, "ERR", res)

    def test_create_many_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            uid = get_uuid()
            uid2 = get_uuid()
            res = connection.create("article", [{"id": uid, "author": uid, "title": uid, "text": uid},
                                                {"id": uid2, "author": uid2, "title": uid2, "text": uid2}])
            self.assertEqual(res.status, "ERR", res)

    def test_update_many_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            uid = get_uuid()
            uid2 = get_uuid()
            connection.create("article", {"author": uid, "title": uid, "text": uid}, record_id=uid)
            res = connection.update("article", [{"author": "inserted", "title": uid, "text": uid},
                                                {"author": "inserted", "title": uid2, "text": uid2},
                                                ], record_id=uid)
            self.assertEqual(res.status, "ERR", res)
            res = connection.update("article", [{"author": "inserted", "title": uid, "text": uid},
                                                {"author": "inserted", "title": uid2, "text": uid2},
                                                ])
            self.assertEqual(res.status, "ERR", res)

    def test_query_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            res = connection.query("SELECT * FROM DATA NOT REALLY AN SQL;")
            self.assertTrue(res.is_error())
            self.assertTrue("Failed to parse query" in res.error)

    def test_ml_export_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            with self.assertRaises(HttpConnectionError):
                connection.ml_export("prediction", "1.0.0")

    def test_select_one_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"select failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.select("article", "any")
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_select_all_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"select all failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.select("article")
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_create_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"create failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.create("article", {}, "any")
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_update_one_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"update one failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.update("article", {}, "any")
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_update_all_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"update all failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.update("article", {})
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_merge_one_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"merge one failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.merge("article", {}, "any")
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_merge_all_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"merge all failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.merge("article", {})
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_delete_one_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"delete one failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.delete("article", "any")
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_patch_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.patch("prediction", [])

    def test_live_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.live("prediction", print)

    def test_custom_live_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.custom_live("LIVE SELECT FROM person;", print)

    def test_kill_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.kill("prediction")

    def test_authenticate_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.authenticate("prediction")

    def test_invalidate_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.invalidate()

    def test_info_failed(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        with db.connect() as connection:
            with self.assertRaises(CompatibilityError):
                connection.info()

    def test_delete_all_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"delete all failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.delete("article")
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_connect_failed(self):
        params = (
            ('Is your SurrealDB started and work on that url?', {'credentials': ('wrong', 'wrong')}),
            ('Is your SurrealDB started and work on that url?', {'credentials': ('root', 'wrong')}),
        )
        for expected, opts in params:
            with self.subTest(f"connect failed on data{opts}"):
                db = Surreal(URL, namespace='test', database='test', use_http=True, **opts)
                with self.assertRaises(SurrealConnectionError) as e:
                    db.connect()
                self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_import_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"import failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                file_path = Path(__file__).parent / "import.surql"
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.import_data(file_path)
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_ml_import_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"ml import failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                file_path = Path(__file__).parent / "empty.surql"
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.ml_import(file_path)
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_export_failed(self):
        for expected, opts in PARAMS:
            with self.subTest(f"export failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.export()
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_ml_export_failed_on_fields(self):
        for expected, opts in PARAMS:
            with self.subTest(f"export failed on data{opts}"):
                db = Surreal(URL, use_http=True, **opts)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.ml_export("prediction", "1.0.0")
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_query_failed_on_headers(self):
        params = (
            ('Not enough permissions to perform this action', {'namespace': 'test', 'database': 'test'}),
            ('Not enough permissions to perform this action', {'credentials': ('user_db', 'user_db')}),
            ('Not enough permissions to perform this action', {'credentials': ('user_ns', 'user_ns')}),
        )
        for expected, opts in params:
            with self.subTest(f"query failed on data{opts}"):
                db = Surreal(URL, use_http=True, namespace='test', )
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.query('INFO FOR ROOT;')
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_import_empty(self):
        params = (
            ("Specify some SQL code to execute", "empty.surql"),
            ("Failed to parse query", "wrong.surql")
        )
        for expected, file in params:
            with self.subTest(f"Import {file}"):
                file_path = Path(__file__).parent / file
                db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
                with db.connect() as connection:
                    with self.assertRaises(HttpConnectionError) as e:
                        connection.import_data(file_path)
                    self.assertTrue(expected in e.exception.args[0], e.exception.args[0])

    def test_db_info_failed_permissions(self):
        surreal = Surreal(URL, credentials=('root', 'root'), use_http=True)
        with surreal.connect() as connection:
            res = connection.db_info()
            self.assertTrue(res.is_error())
            self.assertEqual("INFO FOR DB;", res.query)
            self.assertEqual("Specify a namespace to use", res.error)

    # TODO uncomment after bugfix
    # def test_ml_import_failed_wrong_file(self):
    #     db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
    #     file_path = Path(__file__).parent / "import.srql"
    #     with db.connect() as connection:
    #     with self.assertRaises(HttpClientError):
    #         connection.ml_import(file_path)


if __name__ == '__main__':
    main()
