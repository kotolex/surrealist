from typing import List
from unittest import TestCase, main

from surrealist import Database
from tests.integration_tests.utils import URL


class TestDatabase(TestCase):
    def test_via_http(self):
        with Database(URL, 'test', 'test', ('root', 'root'), use_http=True) as db:
            self.assertEqual("Database(namespace=test, name=test, connected=True)", str(db))
            self.assertTrue("tables" in db.info())
            self.assertTrue(isinstance(db.tables(), List))
            self.assertEqual("test", db.name)
            self.assertEqual("test", db.namespace)
            self.assertEqual("Table(name=person)", str(db.person))
            self.assertEqual("Table(name=person)", str(db.table("person")))

    def test_via_ws(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            self.assertEqual("Database(namespace=test, name=test, connected=True)", str(db))
            self.assertTrue("tables" in db.info())
            self.assertTrue(isinstance(db.tables(), List))
            self.assertEqual("test", db.name)
            self.assertEqual("test", db.namespace)
            self.assertEqual("Table(name=person)", str(db.person))
            self.assertEqual("Table(name=person)", str(db.table("person")))


if __name__ == '__main__':
    main()
