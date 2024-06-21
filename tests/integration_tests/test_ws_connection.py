import time
from unittest import TestCase, main

from surrealist import Surreal, get_uuid, Database
from tests.integration_tests.utils import URL, get_random_series


class TestWebSocketConnection(TestCase):
    def test_connect(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            self.assertTrue(connection.is_connected())
            self.assertEqual("ws://127.0.0.1:8000/rpc", connection._base_url)

    def test_connect_ws(self):
        surreal = Surreal("ws://127.0.0.1:8000/rpc", namespace="test", database="test",
                          credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            self.assertTrue(connection.is_connected())
            self.assertEqual("ws://127.0.0.1:8000/rpc", connection._base_url)

    def test_use(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            res = connection.use('test', 'test')
            self.assertFalse(res.is_error(), res)
            self.assertEqual(None, res.result)

    def test_invalidate(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.invalidate()
            self.assertFalse(res.is_error(), res)
            self.assertIsNone(res.result)
            res = connection.ns_info()
            self.assertTrue(res.is_error(), res)

    # TODO uncomment when bug fix
    # def test_let(self):
    #     surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
    #     with surreal.connect() as connection:
    #         res = connection.let("value151", "42")
    #         self.assertFalse(res.is_error(), res)
    #         self.assertIsNone(res.result)

    def test_unset(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.unset("value151")
            self.assertFalse(res.is_error(), res)
            self.assertIsNone(res.result)

    def test_query(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.query("SELECT * FROM type::table($tb);", {"tb": "article"})
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)

    def test_query_simple(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.query("SELECT * FROM article;")
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)

    def test_select(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.select("article")
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)

    def test_select_with_id(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.select("author", "john")
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)

    def test_select_with_uuid_id(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_uuid()
            data = {"author": "Вася Ëлкин", "title": "øºRusr", "text": "text"}
            res = connection.create("article", {"id": uid, **data})
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)
            self.assertFalse(res.result == [])
            res = connection.select(f"article:⟨{uid}⟩")
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.result, [{"id": f"article:⟨{uid}⟩", **data}])

    def test_create_one(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(22)
            res = connection.create("article", {"id": uid, "author": uid, "title": uid, "text": uid})
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)
            self.assertFalse(res.result == [])
            res = connection.select(f"article:{uid}")
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)

    def test_create_one_with_id(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(25)
            res = connection.create("article", {"author": uid, "title": uid, "text": uid}, uid)
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)
            self.assertFalse(res.result == [])
            res = connection.select("article", uid)
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)

    def test_insert_one(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(27)
            res = connection.insert("article", {"id": uid, "author": uid, "title": uid, "text": uid})
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)
            self.assertFalse(res.result == [])
            res = connection.select(f"article:{uid}")
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)

    def test_insert_bulk(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(21)
            uid2 = get_random_series(33)
            res = connection.insert("article", [{"id": uid, "author": uid, "title": uid, "text": uid},
                                                {"id": uid2, "author": uid2, "title": uid2, "text": uid2}])
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)
            self.assertTrue(len(res.result) == 2)
            res = connection.select(f"article:{uid}")
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)
            res = connection.select(f"article:{uid2}")
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)

    def test_update_one(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(16)
            connection.create("article", {"id": uid, "author": uid, "title": uid, "text": uid})
            res = connection.update("article", {"author": "new", "title": "new", "text": "new"}, uid)
            self.assertFalse(res.is_error(), res)
            self.assertFalse(len(res.result) == 1)
            res = connection.select(f"article:{uid}")
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.result[0]['author'], "new")

    def test_merge_one(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(19)
            connection.create("article", {"id": uid, "author": uid, "title": uid, "text": uid})
            res = connection.merge("article", {"active": True}, uid)
            self.assertFalse(res.is_error(), res)
            self.assertFalse(len(res.result) == 1)
            res = connection.select(f"article:{uid}")
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.result[0]['author'], uid)
            self.assertEqual(res.result[0]['active'], True)

    def test_patch_one(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(24)
            connection.create("article", {"id": uid, "author": uid, "title": uid, "text": uid})
            res = connection.patch("article", [{"op": "replace", "path": "/active", "value": True}], uid)
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.result, {"id": f"article:{uid}", "author": uid, "title": uid, "text": uid,
                                          "active": True})
            res = connection.select(f"article:{uid}")
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.result[0]['author'], uid)
            self.assertEqual(res.result[0]['active'], True)

    def test_patch_with_diff(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(24)
            connection.create("article", {"id": uid, "author": uid, "title": uid, "text": uid, "active": False})
            res = connection.patch("article", [{"op": "replace", "path": "/active", "value": True}], uid, True)
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.result, [{'op': 'replace', 'path': '/active', 'value': True}])
            res = connection.select(f"article:{uid}")
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.result[0]['author'], uid)
            self.assertEqual(res.result[0]['active'], True)

    def test_delete_one(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(31)
            connection.create("article", {"id": uid, "author": uid, "title": uid, "text": uid})
            res = connection.delete("article", uid)
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.result, {"id": f"article:{uid}", "author": uid, "title": uid, "text": uid})
            res = connection.select(f"article:{uid}")
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.result, [])

    def test_delete_all(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(23)
            uid2 = get_random_series(33)
            connection.create("ws_article", {"id": uid, "author": uid, "title": uid, "text": uid})
            connection.create("ws_article", {"id": uid2, "author": uid2, "title": uid2, "text": uid2})
            res = connection.delete("ws_article")
            self.assertFalse(res.is_error(), res)
            res = connection.select(f"ws_article")
            self.assertFalse(res.is_error(), res)
            self.assertEqual(res.result, [])

    def test_live(self):
        a_list = []
        function = lambda mess: a_list.append(mess)
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.live("ws_article", callback=function)
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)
            uid = get_random_series(27)
            opts = {"id": uid, "author": uid, "title": uid, "text": uid}
            connection.create("ws_article", opts)
            time.sleep(0.1)
            self.assertEqual(a_list[0]['result']['action'], 'CREATE')
            self.assertEqual(a_list[0]['result']['result'], {**opts, "id": f"ws_article:{uid}"})

    def test_live_and_kill(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.live("ws_article", callback=lambda x: None)
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)
            token = res.result
            res = connection.kill(token)
            self.assertFalse(res.is_error(), res)
            self.assertIsNone(res.result)

    def test_live_with_diff(self):
        a_list = []
        function = lambda mess: a_list.append(mess)
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.live("ws_article", callback=function, return_diff=True)
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)
            uid = get_random_series(14)
            opts = {"id": uid, "author": uid, "title": uid, "text": uid}
            connection.create("ws_article", opts)
            time.sleep(0.1)
            a_dict = {**opts, "id": f"ws_article:{uid}"}
            self.assertEqual(a_list[0]['result']['action'], 'CREATE')
            self.assertEqual(a_list[0]['result']['result'], [{'op': 'replace', 'path': '/', 'value': a_dict}])

    def test_live_two_queries(self):
        a_list = []
        function = lambda mess: a_list.append(mess)
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            connection.live("ws_article", callback=function)
            connection.live("ws_article2", callback=function)
            uid = get_random_series(27)
            opts = {"id": uid, "author": uid, "title": uid, "text": uid}
            connection.create("ws_article", opts)
            connection.create("ws_article2", opts)
            time.sleep(0.1)
            self.assertEqual(a_list[0]['result']['action'], 'CREATE')
            self.assertEqual(a_list[1]['result']['action'], 'CREATE')
            self.assertEqual(a_list[0]['result']['result'], {**opts, "id": f"ws_article:{uid}"})
            self.assertEqual(a_list[1]['result']['result'], {**opts, "id": f"ws_article2:{uid}"})

    def test_count(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.count("author")
            self.assertFalse(res.is_error())
            self.assertEqual(2, res.result)
            self.assertEqual("SELECT count() FROM author GROUP ALL;", res.query)

    def test_count_is_zero_if_wrong(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.count("wrong")
            self.assertFalse(res.is_error())
            self.assertEqual(0, res.result)

    def test_count_returns_fields(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.count("author:john")
            self.assertFalse(res.is_error())
            self.assertEqual(1, res.result)

    def test_db_info(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.db_info()
            self.assertFalse(res.is_error())
            self.assertTrue('tables' in res.result)
            self.assertEqual("INFO FOR DB;", res.query)
            res = connection.db_info(structured=True)
            self.assertFalse(res.is_error())
            self.assertTrue('tables' in res.result)
            self.assertEqual("INFO FOR DB STRUCTURE;", res.query)

    def test_session_info(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.session_info()
            self.assertFalse(res.is_error())
            self.assertIsNotNone(res.query)
            self.assertEqual({'db': 'test', 'http_origin': 'http://127.0.0.1:8000', 'ip': '127.0.0.1', 'ns': 'test',
                              'scope': None, 'session_id': None}, res.result)

    def test_db_tables(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.db_tables()
            self.assertFalse(res.is_error())
            self.assertTrue('article' in res.result)
            self.assertTrue('person' in res.result)
            self.assertEqual("INFO FOR DB;", res.query)

    def test_remove_table_with_record(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(11)
            tb_name = f"table_{uid}"
            res = connection.create(tb_name, {"name": "John", "status": True})
            self.assertFalse(res.is_error())
            res = connection.remove_table(tb_name)
            self.assertFalse(res.is_error())
            res = connection.db_tables()
            self.assertFalse(res.is_error())
            self.assertFalse(tb_name in res.result)

    def test_remove_table_without_record(self):
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(10)
            tb_name = f"table_{uid}"
            res = connection.create(tb_name, {"id": "john", "name": "John", "status": True})
            self.assertFalse(res.is_error())
            res = connection.delete(tb_name, record_id="john")
            self.assertFalse(res.is_error())
            res = connection.remove_table(tb_name)
            self.assertFalse(res.is_error())
            res = connection.db_tables()
            self.assertFalse(res.is_error())
            self.assertFalse(tb_name in res.result)

    def test_z_custom_live(self):
        a_list = []
        function = lambda mess: a_list.append(mess)
        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.custom_live("LIVE SELECT * FROM ws_person WHERE age > 18;", callback=function)
            self.assertFalse(res.is_error(), res)
            self.assertIsNotNone(res.result)
            token = res.result
            connection.create("ws_person", {"age": 18, "name": "Jane"})
            connection.create("ws_person", {"age": 28, "name": "John"})
            time.sleep(0.1)
            self.assertTrue(len(a_list) == 1, a_list)
            self.assertEqual(a_list[0]['result']['action'], "CREATE")
            self.assertEqual(a_list[0]['result']['result']["age"], 28)
            self.assertEqual(a_list[0]['result']['result']["name"], "John")
            res = connection.kill(token)
            self.assertFalse(res.is_error(), res)

    def test_insert_when_id_exists_returns_existing(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(11)
            res = connection.insert("article", {'id': uid, 'field': 'old'})
            self.assertFalse(res.is_error())
            res = connection.insert("article", {'id': uid, 'field': 'new'})
            self.assertFalse(res.is_error())
            self.assertEqual(res.result, [{'id': f"article:{uid}", 'field': 'old'}])

    def test_update_not_creates_if_not_exists(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(13)
            res = connection.update(f"ws_article:{uid}", {'field': 'old'})
            self.assertFalse(res.is_error())
            res = connection.select(f"ws_article:{uid}")
            self.assertFalse(res.is_error())
            self.assertEqual(res.result, [{'id': f"ws_article:{uid}", 'field': 'old'}])

    def test_upsert_creates_if_not_exists(self):
        with Database(URL, 'test', 'test', credentials=('user_db', 'user_db')) as db:
            uid = get_random_series(13)
            res = db.table("ws_article").upsert(uid).content({'field': 'old'}).run()
            self.assertFalse(res.is_error())
            res = db.table("ws_article").select().by_id(uid).run()
            self.assertFalse(res.is_error())
            self.assertEqual(res.result, [{'id': f"ws_article:{uid}", 'field': 'old'}])

    def test_merge_creates_if_not_exists(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(14)
            res = connection.merge(f"article:{uid}", {'field': 'old'})
            self.assertFalse(res.is_error())
            res = connection.select(f"article:{uid}")
            self.assertFalse(res.is_error())
            self.assertEqual(res.result, [{'id': f"article:{uid}", 'field': 'old'}])

    def test_delete_unexisting(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            uid = get_random_series(14)
            res = connection.delete(f"ws_article:{uid}")
            self.assertEqual(res.result, None)
            self.assertFalse(res.is_error())
            res = connection.delete(uid)
            self.assertFalse(res.is_error())
            self.assertEqual(res.result, [])

    def test_info_root(self):
        surreal = Surreal(URL, credentials=('root', 'root'))
        with surreal.connect() as connection:
            res = connection.root_info()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(res.result), 2)
            res = connection.root_info(structured=True)
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(res.result), 2)

    def test_info_ns(self):
        surreal = Surreal(URL, 'test', credentials=('user_ns', 'user_ns'))
        with surreal.connect() as connection:
            res = connection.ns_info()
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(res.result), 3)
            res = connection.ns_info(structured=True)
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(res.result), 3)

    def test_info_table(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.table_info("author")
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(res.result), 5)
            res = connection.table_info("author", structured=True)
            self.assertFalse(res.is_error(), res)
            self.assertEqual(len(res.result), 5)

    def test_is_table_exists(self):
        surreal = Surreal(URL, 'test', 'test', credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            self.assertTrue(connection.is_table_exists("person"))
            self.assertFalse(connection.is_table_exists("not_exists"))

    def test_nesting_48(self):
        num = 0
        prev = {"name": "first", "age": num, "inner": []}
        for _ in range(48):
            num += 1
            prev = {"name": get_random_series(10), "level": num, "inner": [prev]}

        surreal = Surreal(URL, namespace="test", database="test", credentials=('user_db', 'user_db'))
        with surreal.connect() as connection:
            res = connection.create("ws_article", prev)
            self.assertFalse(res.is_error())


if __name__ == '__main__':
    main()
