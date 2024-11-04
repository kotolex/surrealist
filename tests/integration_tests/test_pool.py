import time
from typing import List
from unittest import TestCase, main

from tests.integration_tests.utils import URL, get_random_series
from surrealist import DatabaseConnectionsPool


text = f"DatabasePool(namespace=test, name=test, connected=True,connections_count=2, min_connections=2, " \
       f"max_connections=10)"


class TestDatabasePool(TestCase):
    def test_check_pool(self):
        for use in (True, False):
            with self.subTest(f"check pool(use_http={use}"):
                with DatabaseConnectionsPool(URL, 'test', 'test', credentials=('user_db', 'user_db'), use_http=use,
                                             min_connections=2, max_connections=10) as db:
                    self.assertEqual(text, str(db))
                    self.assertTrue("tables" in db.info())
                    self.assertTrue(isinstance(db.tables(), List))
                    self.assertEqual("test", db.name)
                    self.assertEqual("test", db.namespace)
                    self.assertEqual(2, db.min_connections)
                    self.assertEqual(10, db.max_connections)
                    self.assertEqual(2, db.connections_count)
                    res = 'http' if use else 'websocket'
                    self.assertEqual(f"Table(name=person, connection with {res} transport)", str(db.person))
                    self.assertEqual(f"Table(name=person, connection with {res} transport)", str(db.table("person")))
                    self.assertTrue(db.some_table.count() >= 0)
                    self.assertEqual(db.raw_query("Return 1;").result, 1)
                    self.assertEqual(db.returns("1").run().result, 1)

    def test_live_and_kill(self):
        a_list = []
        func = lambda mess: a_list.append(mess)
        with DatabaseConnectionsPool(URL, 'test', 'test', credentials=('user_db', 'user_db'),) as db:
            uid = get_random_series(12)
            result = db.live_query(table_name="ws_artile", callback=func).run()
            self.assertFalse(result.is_error())
            live_uid = result.result
            db.ws_artile.create().content({"title": uid}).run()
            time.sleep(0.1)
            self.assertFalse(a_list == [])
            result = db.kill_query(live_uid)
            self.assertFalse(result.is_error())

    def test_live_and_kill_via_table(self):
        a_list = []
        func = lambda mess: a_list.append(mess)
        with DatabaseConnectionsPool(URL, 'test', 'test', credentials=('user_db', 'user_db'),) as db:
            uid = get_random_series(12)
            result = db.table("ws_article").live(callback=func).run()
            self.assertFalse(result.is_error())
            live_uid = result.result
            db.ws_article.create().content({"title": uid}).run()
            time.sleep(0.1)
            self.assertFalse(a_list == [])
            result = db.ws_article.kill(live_uid)
            self.assertFalse(result.is_error())


if __name__ == '__main__':
    main()
