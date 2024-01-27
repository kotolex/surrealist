from pathlib import Path
from unittest import TestCase, main

from py_surreal.surreal import Surreal
from tests.integration_tests.utils import URL, get_uuid


class TestHttpConnection(TestCase):

    def test_is_ready_empty(self):
        db = Surreal(URL, use_http=True, timeout=1)
        connection = db.connect()
        self.assertTrue(connection.is_ready())

    def test_is_ready_full(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        self.assertTrue(connection.is_ready())

    def test_health(self):
        db = Surreal(URL, use_http=True, timeout=1)
        connection = db.connect()
        self.assertEqual("OK", connection.health())

    def test_health_full(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        self.assertEqual("OK", connection.health())

    def test_status(self):
        db = Surreal(URL, use_http=True, timeout=1)
        connection = db.connect()
        self.assertEqual("OK", connection.status())

    def test_status_full(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        self.assertEqual("OK", connection.status())

    def test_version(self):
        db = Surreal(URL, use_http=True, timeout=1)
        connection = db.connect()
        self.assertEqual("surrealdb-1.1.1", connection.version())

    def test_version_full(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        self.assertEqual("surrealdb-1.1.1", connection.version())

    def test_select_one(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        uid = get_uuid()
        connection.create("article", {"id": "exists", "author": uid, "title": uid, "text": uid})
        res = connection.select("article", "exists")
        self.assertTrue(res.result != [])
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.result[0]["id"], "article:exists", res)

    def test_select_one_non_existent(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        res = connection.select("article", "not_exists")
        self.assertTrue(res.result == [])
        self.assertEqual(res.status, "OK")

    def test_select_one_non_existent_table(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        res = connection.select("not_exists", "not_exists")
        self.assertTrue(res.result == [])
        self.assertEqual(res.status, "OK")

    def test_create_one(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        uid = get_uuid()
        res = connection.create("article", {"id": uid, "author": uid, "title": uid, "text": uid})
        self.assertTrue(res.result != [])
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.result[0]["id"], f"article:⟨{uid}⟩", res)
        res = connection.select("article", uid)
        self.assertTrue(res.result != [])
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.result[0]["id"], f"article:⟨{uid}⟩", res)

    def test_create_one_with_id(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        uid = get_uuid()
        res = connection.create("article", {"author": uid, "title": uid, "text": uid}, record_id=uid)
        self.assertTrue(res.result != [])
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.result[0]["id"], f"article:⟨{uid}⟩", res)
        res = connection.select("article", uid)
        self.assertTrue(res.result != [])
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.result[0]["id"], f"article:⟨{uid}⟩", res)

    def test_create_one_no_id(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        uid = get_uuid()
        res = connection.create("article", {"author": uid, "title": uid, "text": uid})
        self.assertTrue(res.result != [])
        self.assertEqual(res.status, "OK")
        self.assertTrue(res.result[0]["id"] != f"article:⟨{uid}⟩", res)

    def test_update_one(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        uid = get_uuid()
        connection.create("article", {"author": uid, "title": uid, "text": uid}, record_id=uid)
        res = connection.update("article", {"author": "inserted", "title": uid, "text": uid}, record_id=uid)
        self.assertTrue(len(res.result) == 1)
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.result[0]["id"], f"article:⟨{uid}⟩", res)
        self.assertEqual(res.result[0]["author"], "inserted", res)
        res = connection.select("article", uid)
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.result[0]["id"], f"article:⟨{uid}⟩", res)
        self.assertEqual(res.result[0]["author"], "inserted", res)

    def test_update_z_all(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        uid = get_uuid()
        connection.create("article", {"author": uid, "title": uid, "text": uid}, record_id=uid)
        res = connection.update("article", {"author": "inserted_all", "title": uid, "text": uid})
        self.assertTrue(len(res.result) > 1)
        self.assertEqual(res.status, "OK")
        self.assertTrue(all(e["author"] == "inserted_all" for e in res.result))

    def test_patch_one(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        uid = get_uuid()
        connection.create("article", {"author": uid, "title": uid, "text": uid}, record_id=uid)
        res = connection.patch("article", {"new_field": "patch"}, record_id=uid)
        self.assertTrue(len(res.result) == 1)
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.result[0]["id"], f"article:⟨{uid}⟩", res)
        self.assertEqual(res.result[0]["new_field"], "patch", res)
        res = connection.select("article", uid)
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.result[0]["id"], f"article:⟨{uid}⟩", res)
        self.assertEqual(res.result[0]["new_field"], "patch", res)

    def test_patch_z_all(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        uid = get_uuid()
        connection.create("article", {"author": uid, "title": uid, "text": uid}, record_id=uid)
        res = connection.patch("article", {"all_field": "patch"})
        self.assertTrue(len(res.result) > 1)
        self.assertEqual(res.status, "OK")
        self.assertTrue(all(e["all_field"] == "patch" for e in res.result))

    def test_query_success(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        res = connection.query("INFO FOR ROOT;")
        self.assertTrue(len(res.result) > 1)
        self.assertEqual(res.status, "OK")

    def test_delete_one(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        uid = get_uuid()
        connection.create("article", {"author": uid, "title": uid, "text": uid}, record_id=uid)
        res = connection.delete("article", record_id=uid)
        self.assertTrue(len(res.result) == 1)
        self.assertEqual(res.status, "OK")
        self.assertEqual(res.result[0]["id"], f"article:⟨{uid}⟩", res)
        res = connection.select("article", uid)
        self.assertEqual(res.status, "OK")
        self.assertTrue(len(res.result) == 0)

    def test_z_delete_all(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        uid = get_uuid()
        connection.create("article", {"author": uid, "title": uid, "text": uid}, record_id=uid)
        res = connection.delete("article")
        self.assertTrue(len(res.result) > 1)
        self.assertEqual(res.status, "OK")
        res = connection.select("article")
        self.assertEqual(res.status, "OK")
        self.assertTrue(len(res.result) == 0)

    def test_export(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        res = connection.export()
        # with open("import.srql", "wt") as file:
        #     file.write(res)
        self.assertTrue("article" in res)
        self.assertTrue("OPTION IMPORT;" in res)

    def test_import(self):
        file_path = Path(__file__).parent / "import.srql"
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        res = connection.import_data(file_path)
        self.assertEqual(res.result, None)
        self.assertEqual(res.status, "OK")

    def test_signin(self):
        params = (
            'root', 'user_db', 'user_ns'
        )
        for type_ in params:
            with self.subTest(f"sign_in as {type_}"):
                db = Surreal(URL, use_http=True)
                connection = db.connect()
                res = connection.signin(type_, type_, namespace='test', database='test')
                self.assertEqual(res.code, 200)
                self.assertEqual(res.details, "Authentication succeeded")

    def test_signup(self):
        db = Surreal(URL, 'test', 'test', use_http=True)
        connection = db.connect()
        res = connection.signup('john:doe', '123456', 'test', 'test', 'user_scope')
        self.assertEqual(res.code, 200)
        self.assertEqual(res.details, "Authentication succeeded")


if __name__ == '__main__':
    main()
