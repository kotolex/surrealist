import threading
import time
from datetime import datetime
from unittest import TestCase, main

from surrealist import OperationOnClosedConnectionError, Surreal, Connection, Database
from tests.integration_tests.utils import URL, get_random_series


class TestUseCases(TestCase):
    connections = []

    @classmethod
    def setUpClass(cls) -> None:
        surreal_ws = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=False)
        surreal_http = Surreal(URL, 'test', 'test', ('root', 'root'), use_http=True)
        con_ws = surreal_ws.connect()
        con_ws.close()
        con_http = surreal_http.connect()
        con_http.close()
        TestUseCases.connections.append(con_ws)
        TestUseCases.connections.append(con_http)

    def test_raise_after_close(self):
        """
        We check here is_connected id False after close, and using method after it raise error
        """
        params = (
            (True, "signin", ["root", "root"]),
            (True, "signup", ["ns", "db", "scope"]),
            (True, "select", ["some"]),
            (True, "create", ["some", {}]),
            (True, "update", ["some", {}]),
            (True, "delete", ["some"]),
            (True, "merge", ["some", {}]),
            (True, "query", ["some"]),
            (True, "patch", ["some", []]),
            (True, "import_data", ["some"]),
            (True, "export", []),
            (True, "ml_import", ["some"]),
            (True, "ml_export", ["some", "some"]),
            (True, "let", ["some", "some"]),
            (True, "unset", ["some"]),
            (True, "use", ["some", "some"]),
            (True, "db_info", []),
            (False, "db_info", []),
            (True, "ns_info", []),
            (False, "ns_info", []),
            (True, "root_info", []),
            (False, "root_info", []),
            (True, "session_info", []),
            (False, "session_info", []),
            (True, "session_info", []),
            (False, "session_info", []),
            (True, "remove_table", []),
            (False, "remove_table", []),
            (True, "count", []),
            (False, "count", []),
            (True, "show_changes", ["a", "b"]),
            (False, "show_changes", ["a", "b"]),
            (True, "is_table_exists", ["a"]),
            (False, "is_table_exists", ["a"]),
            (False, "signin", ["root", "root"]),
            (False, "signup", ["ns", "db", "scope"]),
            (False, "select", ["some"]),
            (False, "create", ["some", {}]),
            (False, "update", ["some", {}]),
            (False, "delete", ["some"]),
            (False, "merge", ["some", {}]),
            (False, "query", ["some"]),
            (False, "let", ["some", "some"]),
            (False, "unset", ["some"]),
            (False, "use", ["some", "some"]),
            (False, "patch", ["some", {}]),
            (False, "insert", ["some", {}]),
            (False, "invalidate", []),
            (False, "authenticate", ["token"]),
            (False, "live", ["token", print]),
            (False, "kill", ["token"]),
        )
        for use_http, method_name, args in params:
            with self.subTest(f"use method {method_name} on connection use_http {use_http}"):
                connection: Connection = TestUseCases.connections[use_http]
                self.assertFalse(connection.is_connected())
                with self.assertRaises(OperationOnClosedConnectionError):
                    getattr(connection, method_name)(*args)

    def test_live_with_two_connections(self):
        """
        We test here live query is linked to one connection only

        """
        a_list = []
        function = lambda mess: a_list.append(mess)
        surreal = Surreal(URL, namespace="test", database="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            with surreal.connect() as connection2:
                res = connection.live("ws_article", callback=function)
                self.assertFalse(res.is_error(), res)
                uid = get_random_series(27)
                opts = {"id": uid, "author": uid, "title": uid, "text": uid}
                connection.create("ws_article", opts)
                time.sleep(0.2)
                self.assertEqual(a_list[0]['result']['action'], 'CREATE')
                self.assertEqual(connection2._client._messages, {})
                self.assertEqual(connection2._client._callbacks, {})

    def test_select_in_threads(self):
        """
        We test here using one connection by two threads
        :return:
        """
        first = []
        second = []
        surreal = Surreal(URL, namespace="test", database="test", credentials=('root', 'root'))
        with surreal.connect() as connection:
            thread1 = threading.Thread(target=lambda: first.append(connection.select("user")), daemon=True)
            thread2 = threading.Thread(target=lambda: second.append(connection.select("article")), daemon=True)
            thread1.start()
            thread2.start()
            thread1.join(3)
            thread2.join(3)
        self.assertNotEqual(first, [])
        self.assertNotEqual(second, [])
        self.assertFalse(first[0].is_error())
        self.assertFalse(second[0].is_error())
        self.assertNotEqual(first, second)

    def test_change_feed(self):
        params = (True, False)
        for use_http in params:
            with self.subTest(f"Change feed use_http={use_http}"):
                surreal = Surreal(URL, 'test', 'test', credentials=('root', 'root'), use_http=use_http)
                with surreal.connect() as connection:
                    count = 0
                    while True:
                        count += 1
                        if count == 10:
                            self.assertTrue(False, "cant wait to changefeed")
                        tm = f'{datetime.utcnow().isoformat("T")}Z'
                        res = connection.show_changes('reading', tm)
                        if not res.is_error():
                            break
                        time.sleep(1)
                    self.assertFalse(res.is_error(), res)
                    story = get_random_series(5)
                    connection.query(f'CREATE reading SET story = "{story}";')
                    res = connection.show_changes('reading', tm)
                    self.assertFalse(res.is_error(), res)
                    self.assertTrue(story in str(res.result))
                    self.assertTrue('changes' in str(res.result))
                    self.assertTrue('update' in str(res.result))
                    self.assertTrue('reading' in str(res.result))

    def test_z_change_feed(self):
        with Database(URL, 'test', 'test', credentials=('root', 'root'), use_http=True) as db:
            tm = f'{datetime.utcnow().isoformat("T")}Z'
            story = get_random_series(5)
            db.table("reading").create().set(story=story).run()
            res = db.table("reading").show_changes().since(tm).run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue(story in str(res.result))
            self.assertTrue('changes' in str(res.result))
            self.assertTrue('update' in str(res.result))
            self.assertTrue('reading' in str(res.result))


if __name__ == '__main__':
    main()
