import time
from typing import List
from unittest import TestCase, main

from tests.integration_tests.utils import URL, get_random_series
from surrealist import Database, WrongCallError


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

    def test_live_and_kill(self):
        a_list = []
        func = lambda mess: a_list.append(mess)
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(14)
            result = db.live_query(table_name="ws_artile", callback=func).run()
            self.assertFalse(result.is_error())
            live_uid = result.result
            db.ws_artile.create().content({"title": uid}).run()
            time.sleep(0.1)
            self.assertFalse(a_list == [])
            result = db.kill_query(live_uid)
            self.assertFalse(result.is_error())

    def test_fail_on_misspell(self):
        with Database(URL, 'test', 'test', ('root', 'root'), use_http=True) as db:
            with self.assertRaises(WrongCallError):
                db.live(1,2)

if __name__ == '__main__':
    main()
