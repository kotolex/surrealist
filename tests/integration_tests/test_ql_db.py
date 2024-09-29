import time
from typing import List
from unittest import TestCase, main

from tests.integration_tests.utils import URL, get_random_series
from surrealist import Database, WrongCallError, Surreal, SurrealConnectionError


class TestDatabase(TestCase):
    def test_via_http(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db'), use_http=True) as db:
            self.assertEqual("Database(namespace=test, name=test, connected=True)", str(db))
            self.assertTrue("tables" in db.info())
            self.assertTrue(isinstance(db.tables(), List))
            self.assertEqual("test", db.name)
            self.assertEqual("test", db.namespace)
            self.assertEqual("Table(name=person)", str(db.person))
            self.assertEqual("Table(name=person)", str(db.table("person")))

    def test_via_ws(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
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
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
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
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db'), use_http=True) as db:
            with self.assertRaises(WrongCallError):
                db.live(1, 2)

    def test_inactive_connections_fails(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'))
        connection = surreal.connect()
        connection.close()
        with self.assertRaises(SurrealConnectionError) as e:
            Database.from_connection(connection)
        self.assertEqual(str(e.exception), "Cant use connection which is not active")

    def test_root_level_fails(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        connection = surreal.connect()
        with self.assertRaises(SurrealConnectionError) as e:
            Database.from_connection(connection)
        self.assertEqual(str(e.exception), "Can use only database level connections, specify namespace and database")

    def test_ns_level_fails(self):
        surreal = Surreal(URL, namespace="test", credentials=('user_ns', 'user_ns'))
        connection = surreal.connect()
        with self.assertRaises(SurrealConnectionError) as e:
            Database.from_connection(connection)
        self.assertEqual(str(e.exception), "Can use only database level connections, specify namespace and database")

    def test_from_connections_works(self):
        with Surreal(URL, credentials=('root', 'root')).connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            self.assertEqual("test", db._namespace)
            self.assertEqual("test", db._database)
            self.assertEqual(connection, db._connection)

    def test_run_function(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.run_function("count")
            self.assertFalse(res.is_error())
            self.assertEqual(1, res.result)



if __name__ == '__main__':
    main()
