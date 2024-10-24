import threading
import time
from datetime import datetime, timezone
from unittest import TestCase, main

from tests.integration_tests.utils import URL, get_random_series
from surrealist import (OperationOnClosedConnectionError, Surreal, Connection, Database, to_surreal_datetime_str,
                        Algorithm)


class TestUseCases(TestCase):
    connections = []

    @classmethod
    def setUpClass(cls) -> None:
        surreal_ws = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'), use_http=False)
        surreal_http = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'), use_http=True)
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
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
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
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
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
        for use_http in (True, False):
            with self.subTest(f"Change feed use_http={use_http}"):
                surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'), use_http=use_http)
                with surreal.connect() as connection:
                    count = 0
                    while True:
                        count += 1
                        if count == 10:
                            self.assertTrue(False, "cant wait to changefeed")
                        tm = to_surreal_datetime_str(datetime.now(timezone.utc))
                        res = connection.show_changes('reading', tm)
                        if not res.is_error():
                            break
                        time.sleep(1)
                    self.assertFalse(res.is_error(), res)
                    story = get_random_series(5)
                    connection.query(f'CREATE reading SET story = "{story}";')
                    res = connection.show_changes('reading', tm)
                    self.assertFalse(res.is_error(), res)
                    self.assertTrue(story in str(res.result), res.result)
                    self.assertTrue('changes' in str(res.result))
                    self.assertTrue('update' in str(res.result))
                    self.assertTrue('reading' in str(res.result))

    def test_z_change_feed(self):
        time.sleep(0.2)
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db'), use_http=True) as db:
            tm = to_surreal_datetime_str(datetime.now())
            story = get_random_series(5)
            db.table("reading").create().set(story=story).run()
            res = db.table("reading").show_changes().since(tm).run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue(story in str(res.result))
            self.assertTrue('changes' in str(res.result))
            self.assertTrue('update' in str(res.result))
            self.assertTrue('reading' in str(res.result))

    # def test_z_change_feed_include_original(self):  # TODO uncomment when SDB will fix INCLUDE ORIGINAL
    #     time.sleep(0.2)
    #     with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
    #         tm = to_surreal_datetime_str(datetime.now(timezone.utc))
    #         time.sleep(1)
    #         story = get_random_series(7)
    #         db.table("include_original").create().set(story=story).run()
    #         res = db.table("include_original").show_changes().since(tm).run()
    #         self.assertFalse(res.is_error(), res)
    #         self.assertTrue(story in str(res.result), res.result)
    #         self.assertEqual(res.result[0]['changes'][0]['current']['story'], story)
    #         self.assertEqual(res.result[0]['changes'][0]['update'], [{'op': 'replace', 'path': '/', 'value': None}])

    def test_use_transaction(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
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
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            events_count = len(db.user.info()["events"])
            uid = get_random_series(6)
            then = db.event.create().set("user = $value.id, time = time::now(), value = $after.email")
            res = db.define_event(f"email_{uid}", table_name="user", then=then).when(
                "$before.email != $after.email").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["events"]), events_count + 1)
            res = db.define_event(f"email_{uid}", table_name="user", then=then).when(
                "$before.email != $after.email").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual(f"The event 'email_{uid}' already exists", res.result, res)
            self.assertEqual(len(db.user.info()["events"]), events_count + 1)
            # now define if exists
            res = db.define_event(f"email_{uid}", table_name="user", then=then).if_not_exists().when(
                "$before.email != $after.email").run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_event(f"email_{uid}", table_name="user").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["events"]), events_count)

    def test_define_event_overwrite(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            events_count = len(db.user.info()["events"])
            uid = get_random_series(6)
            then = db.event.create().set("user = $value.id, time = time::now(), value = $after.email")
            res = db.define_event(f"email_{uid}", table_name="user", then=then).when(
                "$before.email != $after.email").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["events"]), events_count + 1)
            res = db.define_event(f"email_{uid}", table_name="user", then=then).overwrite().when(
                "$before.email != $after.email").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["events"]), events_count + 1)

    def test_remove_non_existent_event(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.remove_event("not_exists", table_name="user").if_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_event("not_exists", table_name="user").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual("The event 'not_exists' does not exist", res.result)

    def test_overwrite_non_existent_event_has_action(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            events_count = len(db.user.info()["events"])
            uid = get_random_series(6)
            then = db.event.create().set("user = $value.id, time = time::now(), value = $after.email")
            res = db.define_event(f"evet_{uid}", table_name="user", then=then).overwrite().when(
                "$before.email != $after.email").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["events"]), events_count+1)

    def test_define_user_and_remove(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            uid = get_random_series(8)
            count = len(db.info()["users"])
            res = db.define_user(f"user_{uid}").password("123456").run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue(len(db.info()["users"]), count + 1)
            res = db.define_user(f"user_{uid}").password("123456").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual(f"The user 'user_{uid}' already exists in the database 'test'", res.result, res)
            self.assertTrue(len(db.info()["users"]), count + 1)
            # now define if exists
            res = db.define_user(f"user_{uid}").password("123456").if_not_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_user(f"user_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue(len(db.info()["users"]), count)

    def test_define_user_and_overwrite(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            uid = get_random_series(8)
            count = len(db.info()["users"])
            res = db.define_user(f"user_{uid}").password("123456").run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue(len(db.info()["users"]), count + 1)
            res = db.define_user(f"user_{uid}").password("123123").overwrite().run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue(len(db.info()["users"]), count + 1)

    def test_overwrite_non_existent_user_no_action(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            uid = get_random_series(8)
            db = Database.from_connection(connection)
            count = len(db.info()["users"])
            res = db.define_user(f"user_{uid}").password("123123").overwrite().run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue(len(db.info()["users"]), count)

    def test_remove_non_existent_user(self):
        surreal = Surreal(URL, credentials=("root", "root"), use_http=True)
        with surreal.connect() as ws_connection:  # create context manager, it will close connection for us
            ws_connection.use("test", "test")
            db = Database.from_connection(ws_connection)
            res = db.remove_user("not_exists").if_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_user("not_exists").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual("The user 'not_exists' does not exist in the database 'test'", res.result)

    def test_define_param_and_remove(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            uid = get_random_series(8)
            res = db.define_param(f"param_{uid}", 1000).run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(1000, db.raw_query(f"RETURN $param_{uid};").result)
            res = db.define_param(f"param_{uid}", 1000).run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual(f"The param '$param_{uid}' already exists", res.result, res)
            res = db.define_param(f"param_{uid}", 1000).if_not_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_param(f"param_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(None, db.raw_query(f"RETURN $param_{uid};").result)

    def test_define_param_and_overwrite(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            uid = get_random_series(8)
            res = db.define_param(f"param_{uid}", 1000).run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(1000, db.raw_query(f"RETURN $param_{uid};").result)
            res = db.define_param(f"param_{uid}", 5000).overwrite().run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(5000, db.raw_query(f"RETURN $param_{uid};").result)

    def test_overwrite_non_existent_param_has_action(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.define_param(f"param_non_exists", 1000).overwrite().run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(1000, db.raw_query(f"RETURN $param_non_exists;").result)

    def test_remove_non_existent_param(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.remove_param("not_exists").if_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_param("not_exists").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual("The param '$not_exists' does not exist", res.result)

    def test_define_analyzer_and_remove(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            uid = get_random_series(4)
            count = len(db.info()["analyzers"])
            res = db.define_analyzer(f"anal_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["analyzers"]), count + 1)
            res = db.define_analyzer(f"anal_{uid}").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual(f"The analyzer 'anal_{uid}' already exists", res.result, res)
            res = db.define_analyzer(f"anal_{uid}").if_not_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_analyzer(f"anal_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["analyzers"]), count)

    def test_define_analyzer_and_overwrite(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            uid = get_random_series(4)
            count = len(db.info()["analyzers"])
            res = db.define_analyzer(f"anal_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["analyzers"]), count + 1)
            res = db.define_analyzer(f"anal_{uid}").overwrite().run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["analyzers"]), count+1)

    def test_overwrite_non_existent_analyzer_has_action(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            count = len(db.info()["analyzers"])
            res = db.define_analyzer(f"anal_not_exists").overwrite().run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["analyzers"]), count+1)

    def test_remove_non_existent_analyzer(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.remove_analyzer("not_exists").if_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_analyzer("not_exists").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual("The analyzer 'not_exists' does not exist", res.result)

    def test_define_scope_and_remove(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            uid = get_random_series(6)
            count = len(db.info()["accesses"])
            create = db.user.create().set("email = $email, pass = crypto::argon2::generate($pass)")
            select = db.user.select().where("email = $email AND crypto::argon2::compare(pass, $pass)")
            res = db.define_scope(f"scope_{uid}", "24h", signup=create, signin=select).run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count + 1)
            res = db.define_scope(f"scope_{uid}", "24h", signup=create, signin=select).run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual(f"The access method 'scope_{uid}' already exists in the database 'test'", res.result, res)
            self.assertEqual(len(db.info()["accesses"]), count + 1)
            res = db.define_scope(f"scope_{uid}", "24h", signup=create, signin=select).if_not_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_access(f"scope_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count)

    def test_define_access_jwt_and_remove(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            uid = get_random_series(7)
            count = len(db.info()["accesses"])
            res = db.define_access_jwt(f"access_{uid}").algorithm(Algorithm.HS512, "some_key").duration("24h").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count + 1)
            res = db.define_access_jwt(f"access_{uid}").algorithm(Algorithm.HS512, "some_key").duration("24h").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual(f"The access method 'access_{uid}' already exists in the database 'test'", res.result, res)
            self.assertEqual(len(db.info()["accesses"]), count + 1)
            res = db.define_access_jwt(f"access_{uid}").algorithm(Algorithm.HS512, "some_key").duration(
                "24h").if_not_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_access(f"access_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count)

    def test_define_access_jwt_and_overwrite(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            uid = get_random_series(7)
            count = len(db.info()["accesses"])
            res = db.define_access_jwt(f"access_{uid}").algorithm(Algorithm.HS512, "some_key").duration("24h").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count + 1)
            res = db.define_access_jwt(f"access_{uid}").algorithm(Algorithm.HS512, "other_key").duration("11h").overwrite().run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count +1)
            self.assertTrue("11h" in db.info()["accesses"][f"access_{uid}"],  db.info()["accesses"][f"access_{uid}"])

    def test_overwrite_non_existent_access_jwt_has_action(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            count = len(db.info()["accesses"])
            res = db.define_access_jwt(f"access_not_exists").algorithm(Algorithm.HS512, "some_key").duration("24h").overwrite().run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count+1)

    def test_define_access_record_and_remove(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            uid = get_random_series(8)
            count = len(db.info()["accesses"])
            res = db.define_access_record(f"access_{uid}").duration_for_token("24h").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count + 1)
            res = db.define_access_record(f"access_{uid}").duration_for_token("24h").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual(f"The access method 'access_{uid}' already exists in the database 'test'", res.result, res)
            self.assertEqual(len(db.info()["accesses"]), count + 1)
            res = db.define_access_record(f"access_{uid}").duration_for_token("24h").if_not_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_access(f"access_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count)

    def test_define_access_masked_in_info(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            uid = get_random_series(8)
            res = db.define_access_jwt(f"access_{uid}").algorithm(Algorithm.HS512, "some_key").run()
            self.assertFalse(res.is_error(), res)
            self.assertTrue("some_key" not in str(db.info()["accesses"]))

    def test_define_index_and_remove(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            ind_count = len(db.user.info()["indexes"])
            uid = get_random_series(7)
            db.define_analyzer("ascii").run()
            res = db.define_index(f"index_{uid}", "user").columns("name").search_analyzer("ascii").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["indexes"]), ind_count + 1)
            res = db.define_index(f"index_{uid}", "user").columns("name").search_analyzer("ascii").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual(f"The index 'index_{uid}' already exists", res.result, res)
            self.assertEqual(len(db.user.info()["indexes"]), ind_count + 1)
            res = db.define_index(f"index_{uid}", "user").if_not_exists().columns("name").search_analyzer("ascii").run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_index(f"index_{uid}", table_name="user").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["indexes"]), ind_count)

    def test_define_mtree_index_and_remove(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            uid = get_random_series(8)
            ind_count = len(db.table(f"user{uid}").info()["indexes"])
            res = db.define_index(f"index_{uid}", f"user{uid}").columns("name").mtree(4).distance_euclidean().run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.table(f"user{uid}").info()["indexes"]), ind_count + 1)
            res = db.remove_index(f"index_{uid}", table_name=f"user{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.table(f"user{uid}").info()["indexes"]), ind_count)

    def test_define_hnsw_index_and_remove(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            uid = get_random_series(11)
            ind_count = len(db.table(f"user{uid}").info()["indexes"])
            res = db.define_index(f"index_{uid}", f"user{uid}").columns("name").hnsw(4).distance_euclidean().efc(
                150).max_connections(2).run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.table(f"user{uid}").info()["indexes"]), ind_count + 1)
            res = db.remove_index(f"index_{uid}", table_name=f"user{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.table(f"user{uid}").info()["indexes"]), ind_count)

    def test_remove_non_existent_index(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.remove_index("not_exists", table_name="user").if_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_index("not_exists", table_name="user").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual("The index 'not_exists' does not exist", res.result)

    def test_define_index_concurrently_and_rebuilds(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            uid = get_random_series(9)
            db.define_analyzer("ascii2").run()
            res = db.define_index(f"index_{uid}", "user").columns("name").search_analyzer("ascii2").concurrently().run()
            self.assertFalse(res.is_error(), res)
            res = db.rebuild_index(f"index_{uid}", "user").run()
            self.assertFalse(res.is_error(), res)
            res = db.rebuild_index(f"index_{uid}", "user", if_exists=True).run()
            self.assertFalse(res.is_error(), res)
            res = db.rebuild_index(f"index_WRONG", "user", if_exists=True).run()
            self.assertFalse(res.is_error(), res)
            res = db.rebuild_index(f"index_WRONG", "user").run()
            self.assertTrue(res.is_error(), res)

    def test_define_failed_no_analyzer(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            uid = get_random_series(8)
            res = db.define_index(f"index_{uid}", "user").columns("name").search_analyzer("non-exists").run()
            self.assertTrue(res.is_error(), res)

    def test_iterator(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            iterator = db.table("user").select().iter(limit=3)
            count = db.user.count()
            total = 0
            for result in iterator:
                records = result.count()
                self.assertTrue(records <= 3)
                total += records
            self.assertEqual(total, count)

    def test_define_token_and_remove(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            uid = get_random_series(8)
            count = len(db.info()["accesses"])
            val = "sNSYneezcr8kqphfOC6NwwraUHJCVAt0XjsRSNmssBaBRh3WyMa9TRfq8ST7fsU2H2kGiOpU4GbAF1bCiXmM1b3JGgleBzz7rsrz6VvYEM4q3CLkcO8CMBIlhwhzWmy8"
            res = db.define_token(f"token_{uid}", Algorithm.HS512, value=val).run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count + 1)
            res = db.define_token(f"token_{uid}", Algorithm.HS512, value=val).run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual(f"The access method 'token_{uid}' already exists in the database 'test'", res.result, res)
            res = db.define_token(f"token_{uid}", Algorithm.HS512, value=val).if_not_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_access(f"token_{uid}").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.info()["accesses"]), count)

    def test_define_relate(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.relate("author:john->write->ws_article:main").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.get("in"), "author:john")
            self.assertEqual(res.get("out"), "ws_article:main")

    def test_define_table(self):
        uid = get_random_series(8)
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            from surrealist import Where
            select = Where(published=True).OR(user="$auth.id")
            create = Where(user="$auth.id")
            delete = Where(user="$auth.id").OR("$auth.admin = true")
            res = db.define_table(f"post_{uid}").schemaless().permissions_for(select=select, create=create,
                                                                              update=create, delete=delete).run()
            self.assertFalse(res.is_error(), res)

    def test_define_tables_with_types(self):
        uid = get_random_series(11)
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.define_table(f"any_type_{uid}").type_any().run()
            self.assertFalse(res.is_error(), res)
            res = db.define_table(f"normal_type_{uid}").type_normal().run()
            self.assertFalse(res.is_error(), res)
            res = db.define_table(f"relation_type_{uid}").type_relation().run()
            self.assertFalse(res.is_error(), res)
            res = db.define_table(f"relation_type_from_to_{uid}").type_relation(("user", "post")).run()
            self.assertFalse(res.is_error(), res)
            res = db.define_table(f"relation_type_in_out_{uid}").type_relation(("user", "post"),
                                                                               use_from_to=False).run()
            self.assertFalse(res.is_error(), res)

    def test_define_field_and_remove(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            ind_count = len(db.user.info()["fields"])
            uid = get_random_series(5)
            res = db.define_field(f"field_{uid}", "user").type("bool").read_only().run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["fields"]), ind_count + 1)
            res = db.define_field(f"field_{uid}", "user").type("bool").read_only().run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual(f"The field 'field_{uid}' already exists", res.result, res)
            self.assertEqual(len(db.user.info()["fields"]), ind_count + 1)
            res = db.define_field(f"field_{uid}", "user").type("bool").read_only().if_not_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_field(f"field_{uid}", table_name="user").run()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(db.user.info()["fields"]), ind_count)

    def test_remove_non_existent_field(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.remove_field("not_exists", table_name="user").if_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_field("not_exists", table_name="user").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual("The field 'not_exists' does not exist", res.result)

    def test_remove_non_existent_table(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.remove_table("not_exists").if_exists().run()
            self.assertFalse(res.is_error(), res)
            res = db.remove_table("not_exists").run()
            self.assertTrue(res.is_error(), res)
            self.assertEqual("The table 'not_exists' does not exist", res.result)

    def test_remove_non_existent_db(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            connection.use("test", "test")
            res = connection.query("REMOVE DB IF EXISTS surrealdb_non_exist;")
            self.assertFalse(res.is_error(), res)
            res = connection.query("REMOVE DB surrealdb_non_exist;")
            self.assertTrue(res.is_error(), res)
            self.assertEqual("The database 'surrealdb_non_exist' does not exist", res.result)

    def test_bug_where(self):  # https://github.com/surrealdb/surrealdb/issues/3510
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            db.table("a").create(record_id=1).run()
            db.table("b").create(record_id=1).set(link="a:1", num=1).run()
            res1 = db.table("b").select("link.*").run().result
            res2 = db.table("b").select("link.*").where("num = 1").run().result
            self.assertEqual(res1, res2)

    # def test_bug_index_cant_use_datetime(self):  # https://github.com/surrealdb/surrealdb/issues/2939
    #     with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
    #         db.define_index("idx_first", table_name="series").columns("f_aired")
    #         r = db.table("series").select("id, f_aired").where('f_aired > "2024-10-01T00:00:00Z"').explain().run()
    #         print(r)
    #         self.assertTrue("Unsupported" not in r.result[1]['detail']['reason'])
    #
    # def test_bug_index_unsupport_uuid(self):  # https://github.com/surrealdb/surrealdb/issues/2939
    #     with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
    #         r = db.table("sessions").select().where(
    #             'sessionUid = "00ad70db-f435-442e-9012-1cd853102084"').explain().run()
    #         self.assertTrue("Unsupported" not in r.result[1]['detail']['reason'])

    def test_for_full_text_search(self):  # https://surrealdb.com/docs/surrealdb/reference-guide/full-text-search
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            # DEFINE ANALYZER custom_analyzer TOKENIZERS blank FILTERS lowercase, snowball(english);
            db.define_analyzer("custom_analyzer").tokenizer_blank().filter_snowball("english").filter_lowercase().run()
            # DEFINE INDEX book_title ON book FIELDS title SEARCH ANALYZER custom_analyzer BM25;
            # DEFINE INDEX book_content ON book FIELDS content SEARCH ANALYZER custom_analyzer BM25;
            db.define_index("book_title", "book").fields("title").search_analyzer("custom_analyzer").bm25().run()
            db.define_index("book_content", "book").fields("content").search_analyzer("custom_analyzer").bm25().run()
            res = db.book.select().where("content @@ 'tools'").run()
            self.assertFalse(res.is_error(), res)

    # def test_live_no_results_on_transaction_fail(self):  # https://github.com/surrealdb/surrealdb/issues/3742
    #     """
    #     We test here live query is not getting updates on transaction fail.
    #
    #     """
    #     # TODO add cancel transaction
    #     a_list = []
    #     function = lambda mess: a_list.append(mess)
    #     surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
    #     with surreal.connect() as connection:
    #         res = connection.live("player", callback=function)
    #         self.assertFalse(res.is_error(), res)
    #         tr = """
    #             BEGIN TRANSACTION;
    #
    #   LET $p = (CREATE player CONTENT {
    #     name: "barbar",
    #   } RETURN id)[0];
    #
    #   THROW $p.id;
    #   COMMIT TRANSACTION;
    #             """
    #         res = connection.query(tr)
    #         time.sleep(0.2)
    #         self.assertEqual(res.result[0]["status"], "ERR", res)
    #         self.assertEqual(a_list, [], a_list)

    def test_insert_bulk_checked_by_lq(self):
        a_list = []
        function = lambda mess: a_list.append(mess)
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.live("article", callback=function)
            self.assertFalse(res.is_error(), res)
            uid = get_random_series(21)
            uid2 = get_random_series(33)
            res = connection.insert("article", [{"id": uid, "author": uid, "title": uid, "text": uid},
                                                {"id": uid2, "author": uid2, "title": uid2, "text": uid2}])
            self.assertFalse(res.is_error(), res)
            time.sleep(0.2)
            self.assertEqual(a_list[0]["result"]["action"], "CREATE", a_list)
            self.assertEqual(a_list[1]["result"]["action"], "CREATE", a_list)
            self.assertEqual(a_list[0]["result"]["result"]["author"], uid, a_list)
            self.assertEqual(a_list[1]["result"]["result"]["author"], uid2, a_list)

    def test_continue(self):  # https://surrealdb.com/docs/surrealdb/surrealql/statements/continue
        text = """
            FOR $person IN (SELECT id, age FROM person) {
	IF ($person.age < 18) {
		CONTINUE;
	};

	UPDATE $person.id SET can_vote = true;
};
   """
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            connection.create("person", {"id": "John", "age": 16, "name": "John"})
            connection.create("person", {"id": "Jane", "age": 20, "name": "Jane"})
            res = connection.query(text)
            self.assertFalse(res.is_error(), res)
            res = connection.select("person:Jane")
            self.assertEqual(res.result[0]["can_vote"], True, res)
            res = connection.select("person:John")
            self.assertTrue("can_vote" not in res.result[0], res)

    def test_throw(self):  # https://surrealdb.com/docs/surrealdb/surrealql/statements/throw
        text = 'THROW "some error message";'
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.query(text)
            self.assertTrue(res.is_error(), res)
            self.assertEqual(res.result, "An error occurred: some error message", res)

    def test_array_clump_fails(self):
        text = 'RETURN array::clump([0, 1, 2, 3], 0);'
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.query(text)
            self.assertTrue(res.is_error(), res)

    def test_alter_table(self):
        uid = get_random_series(7)
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            res = db.define_table(f"alter_{uid}").run()
            self.assertFalse(res.is_error(), res)
            res = db.alter_table(f"alter_{uid}").run()
            self.assertFalse(res.is_error(), res)
            res = db.alter_table(f"alter_{uid}").drop().comment("test").run()
            self.assertFalse(res.is_error(), res)
            res = db.alter_table(f"alter_{uid}").schemaless().run()
            self.assertFalse(res.is_error(), res)
            res = db.alter_table(f"alter_{uid}").schemafull().run()
            self.assertFalse(res.is_error(), res)
            res = db.alter_table(f"alter_{uid}").permissions_full().run()
            self.assertFalse(res.is_error(), res)

    def test_datetimes_field(self):
        surreal = Surreal(URL, credentials=("root", "root"))
        with surreal.connect() as connection:
            connection.use("test", "test")
            db = Database.from_connection(connection)
            db.define_field("created_at", "datetime_table").type("datetime").default("time::now()").permissions_full().run()
            tm = to_surreal_datetime_str(datetime.now(timezone.utc))
            result = connection.create("datetime_table", {'name': "zzz", 'age': 44, 'created_at': tm})
            self.assertFalse(result.is_error(), result)
            result = db.datetime_table.create().content({'name': "xxx", 'age': 22, 'created_at': tm}).run()
            self.assertFalse(result.is_error(), result)


if __name__ == '__main__':
    main()
