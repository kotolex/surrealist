import time
from unittest import TestCase, main

from surrealist import Database
from tests.integration_tests.utils import URL, get_random_series


class TestTable(TestCase):
    def test_count(self):
        with Database(URL, 'test', 'test', ('root', 'root'), use_http=True) as db:
            author = db.author
            self.assertEqual(2, author.count())
            author = db.table("author")
            self.assertEqual(2, author.count())

    def test_select(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            result = db.table("author").select().run()
            self.assertFalse(result.is_error())
            self.assertEqual(result.result[0]["id"], "author:john")

    def test_create(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(11)
            result = db.table("ql_article").create(uid).content({"author": "author:john", "title": uid}).return_none(). \
                run()
            self.assertFalse(result.is_error())
            self.assertEqual(result.result, [])
            result = db.ql_article.select().by_id(uid).run()
            self.assertFalse(result.is_error())
            self.assertEqual(result.result[0]["title"], uid)

    def test_delete(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(12)
            result = db.table("ql_article").create(uid).content({"title": uid}).return_none().run()
            self.assertFalse(result.is_error())
            self.assertEqual(result.result, [])
            result = db.table("ql_article").delete(uid).return_none().run()
            self.assertFalse(result.is_error())
            self.assertEqual(result.result, [], result)
            result = db.ql_article.select().by_id(uid).run()
            self.assertFalse(result.is_error())
            self.assertEqual(result.result, [])

    def test_delete_all(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(12)
            table = db.table("qld_article")
            table.create().content({"title": uid}).return_none().run()
            table.create().content({"title": uid}).return_none().run()
            count = table.count()
            result = table.delete_all()
            self.assertFalse(result.is_error())
            self.assertEqual(result.result, [])
            self.assertEqual(table.count(), 0)
            self.assertTrue(table.count() != count)

    def test_remove_or_drop(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(13)
            table = db.table(f"test_{uid}")
            table2 = db.table(f"test_{uid}2")
            table.create().content({"title": uid}).return_none().run()
            table2.create().content({"title": uid}).return_none().run()
            result = table.drop()
            result2 = table2.remove()
            self.assertFalse(result.is_error())
            self.assertFalse(result2.is_error())
            self.assertTrue(f"test_{uid}" not in db.tables())
            self.assertTrue(f"test_{uid}2" not in db.tables())

    def test_live_and_kill(self):
        a_list = []
        func = lambda mess: a_list.append(mess)
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(14)
            table = db.table("ws_artile")
            result = table.live(func).run()
            self.assertFalse(result.is_error())
            live_uid = result.result
            table.create().content({"title": uid}).run()
            time.sleep(0.1)
            self.assertFalse(a_list == [])
            result = table.kill(live_uid)
            self.assertFalse(result.is_error())

    def test_live_and_kill_with_cond(self):
        a_list = []
        func = lambda mess: a_list.append(mess)
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(14)
            table = db.table("ws_person")
            result = table.live(func).alias("title", "TITLE").where("age > 22").run()
            self.assertFalse(result.is_error())
            live_uid = result.result
            table.create().content({"title": uid, "age": 22}).run()
            table.create().content({"title": "new", "age": 40}).run()
            time.sleep(0.1)
            self.assertFalse(a_list == [])
            self.assertTrue("new" in str(a_list))
            self.assertTrue("TITLE" in str(a_list))
            result = table.kill(live_uid)
            self.assertFalse(result.is_error())

    def test_insert(self):
        with Database(URL, 'test', 'test', ('root', 'root'), use_http=True) as db:
            uid = get_random_series(10)
            result = db.person.insert(("name", "age"), (uid, 33)).run()
            self.assertFalse(result.is_error())
            self.assertEqual(result.result[0]["age"], 33)
            self.assertEqual(result.result[0]["name"], uid)

    def test_update(self):
        with Database(URL, 'test', 'test', ('root', 'root')) as db:
            uid = get_random_series(14)
            result = db.table("ql_article").create(uid).content({"author": "author:john", "title": uid}).return_none(). \
                run()
            self.assertFalse(result.is_error())
            result = db.table("ql_article").update(uid).only().merge({"active": True}).run()
            self.assertFalse(result.is_error())
            self.assertTrue(result.result["active"])


if __name__ == '__main__':
    main()
