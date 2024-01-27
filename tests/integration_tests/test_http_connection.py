from unittest import TestCase, main
from py_surreal.surreal import Surreal
from tests.integration_tests.utils import URL


class TestHttpConnection(TestCase):

    def test_is_ready(self):
        db = Surreal(URL, 'None', 'None', use_http=True, timeout=1)
        connection = db.connect()
        self.assertTrue(connection.is_ready())

    def test_health(self):
        db = Surreal(URL, 'None', 'None', use_http=True, timeout=1)
        connection = db.connect()
        self.assertEqual("OK", connection.health())

    def test_status(self):
        db = Surreal(URL, 'None', 'None', use_http=True, timeout=1)
        connection = db.connect()
        self.assertEqual("OK", connection.status())

    def test_version(self):
        db = Surreal(URL, 'None', 'None', use_http=True, timeout=1)
        connection = db.connect()
        self.assertEqual("surrealdb-1.1.1", connection.version())

    def test_select_one(self):
        db = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        connection = db.connect()
        res = connection.select("article", "fbk43xn5vdb026hdscnz")
        self.assertTrue(res.result != [])
        self.assertEqual(res.status,  "OK")

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


if __name__ == '__main__':
    main()
