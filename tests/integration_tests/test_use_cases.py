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
            (True, "table_info", ["a"]),
            (False, "table_info", ["a"]),
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
        time.sleep(0.2)
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

    def test_use_transaction(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            author = db.table("t_author")
            book = db.table("t_book")
            counter = db.table("t_counter")
            uid = get_random_series(14)
            create_author = author.create().content({"name": uid, "id": uid})
            create_book = book.create().content({"title": "Title", "text": uid, "author": f"author:{uid}"})
            counter_inc = counter.update("author_count").set("count +=1")
            transaction = db.transaction([create_author, create_book, counter_inc])
            res = transaction.run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue(len(res.result) == 3)

    def test_define_event_and_remove(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            events_count = len(db.user.info()["events"])
            uid = get_random_series(6)
            then = db.event.create().set("user = $value.id, time = time::now(), value = $after.email")
            res = db.define_event(f"email_{uid}", table_name="user", then=then).when(
                "$before.email != $after.email").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["events"]), events_count + 1)
            res = db.remove_event(f"email_{uid}", table_name="user").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["events"]), events_count)

    def test_define_user_and_remove(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(8)
            count = len(db.info()["users"])
            res = db.define_user(f"user_{uid}", password="123456").run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue(len(db.info()["users"]), count + 1)
            res = db.remove_user(f"user_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue(len(db.info()["users"]), count)

    def test_define_param_and_remove(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(8)
            res = db.define_param(f"param_{uid}", 1000).run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(1000, db.raw_query(f"RETURN $param_{uid};").result)
            res = db.remove_param(f"param_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(None, db.raw_query(f"RETURN $param_{uid};").result)

    def test_define_analyzer_and_remove(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(4)
            count = len(db.info()["analyzers"])
            res = db.define_analyzer(f"anal_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["analyzers"]), count + 1)
            res = db.remove_analyzer(f"anal_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["analyzers"]), count)

    def test_define_scope_and_remove(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(6)
            count = len(db.info()["scopes"])
            create = db.user.create().set("email = $email, pass = crypto::argon2::generate($pass)")
            select = db.user.select().where("email = $email AND crypto::argon2::compare(pass, $pass)")
            res = db.define_scope(f"scope_{uid}", "24h", signup=create, signin=select).run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["scopes"]), count + 1)
            res = db.remove_scope(f"scope_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["scopes"]), count)

    def test_define_index_and_remove(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            ind_count = len(db.user.info()["indexes"])
            uid = get_random_series(7)
            db.define_analyzer("ascii").run()
            res = db.define_index(f"index_{uid}", "user").columns("name").search_analyzer("ascii").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["indexes"]), ind_count + 1)
            res = db.remove_index(f"index_{uid}", table_name="user").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["indexes"]), ind_count)

    def test_define_failed_no_analyzer(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(8)
            res = db.define_index(f"index_{uid}", "user").columns("name").search_analyzer("non-exists").run()
            self.assertTrue(res.is_error(), res)

    def test_iterator(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            iterator = db.table("user").select().iter(limit=3)
            count = db.user.count()
            total = 0
            for result in iterator:
                records = result.count()
                self.assertTrue(records <= 3)
                total += records
            self.assertEqual(total, count)

    def test_define_token_and_remove(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(8)
            count = len(db.info()["tokens"])
            val = "sNSYneezcr8kqphfOC6NwwraUHJCVAt0XjsRSNmssBaBRh3WyMa9TRfq8ST7fsU2H2kGiOpU4GbAF1bCiXmM1b3JGgleBzz7rsrz6VvYEM4q3CLkcO8CMBIlhwhzWmy8"
            res = db.define_token(f"token_{uid}", "HS512", value=val).run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["tokens"]), count + 1)
            res = db.remove_token(f"token_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["tokens"]), count)

    def test_define_relate(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            res = db.relate("author:john->write->ws_article:main").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.get("in"), "author:john")
            self.assertEqual(res.get("out"), "ws_article:main")

    def test_define_table(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            from surrealist import Where
            select = Where(published=True).OR(user="$auth.id")
            create = Where(user="$auth.id")
            delete = Where(user="$auth.id").OR("$auth.admin = true")
            res = db.define_table("post").schemaless().permissions_for(select=select, create=create, update=create,
                                                                       delete=delete).run()
            self.assertFalse(res.is_error())

    def test_define_field_and_remove(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            ind_count = len(db.user.info()["fields"])
            uid = get_random_series(5)
            res = db.define_field(f"field_{uid}", "user").type("bool").read_only().run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["fields"]), ind_count + 1)
            res = db.remove_field(f"field_{uid}", table_name="user").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["fields"]), ind_count)

    def test_bug_where(self):  # https://github.com/surrealdb/surrealdb/issues/3510
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            db.table("a").create(record_id=1).run()
            db.table("b").create(record_id=1).set(link="a:1", num=1).run()
            res1 = db.table("b").select("link.*").run().result
            res2 = db.table("b").select("link.*").where("num = 1").run().result
            self.assertEqual(res1, res2)
    #
    # def test_bug_index_cant_use_datetime(self):  # https://github.com/surrealdb/surrealdb/issues/2939
    #     with Database(URL, 'test', 'test', ('root', 'root')) as db:
    #         db.define_index("idx_first", table_name="series").columns("f_aired")
    #         r = db.table("series").select("id, f_aired").where('f_aired > "2024-10-01T00:00:00Z"').explain().run()
    #         self.assertTrue("Unsupported" not in r.result[1]['detail']['reason'])
    #
    # def test_bug_index_unsupport_uuid(self):  # https://github.com/surrealdb/surrealdb/issues/2939
    #     with Database(URL, 'test', 'test', ('root', 'root')) as db:
    #         r = db.table("sessions").select().where(
    #             'sessionUid = "00ad70db-f435-442e-9012-1cd853102084"').explain().run()
    #         self.assertTrue("Unsupported" not in r.result[1]['detail']['reason'])

    # def test_bug_index(self): # https://github.com/surrealdb/surrealdb/issues/3530
    #     with Database(URL, 'test', 'test', ('root', 'root')) as db:
    #         # DEFINE ANALYZER custom_analyzer TOKENIZERS blank FILTERS lowercase, snowball(english);
    #         r = db.define_analyzer("custom_analyzer").tokenizers("blank").filters("lowercase, snowball(english)").run()
    #         self.assertFalse(r.is_error())
    #         # DEFINE INDEX book_idx ON book FIELDS title, content SEARCH ANALYZER custom_analyzer BM25;
    #         r = db.define_index("book_idx", "book").fields("title, content").search_analyzer("custom_analyzer",
    #                                                                                          highlights=False).run()
    #         self.assertFalse(r.is_error())
    #         # SELECT * FROM book WHERE content @@ 'tools';
    #         r = db.book.select().where("content @@ 'tools'").run()
    #         self.assertFalse(r.is_error())
    #         self.assertEqual([], r.result)
    #         db.table("book").create().set(title="Some tools", content="A book about tools in programming").run()
    #         r = db.book.select().where("content @@ 'tools'").run()
    #         self.assertFalse(r.is_error(), r)
    #         self.assertEqual([], r.result)



if __name__ == '__main__':
    main()
