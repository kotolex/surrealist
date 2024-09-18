from unittest import TestCase, main

from surrealist import Database
from surrealist.ql.statements.select import Select


class TestSelect(TestCase):
    def test_select_default(self):
        select = Select(None, "person")
        self.assertEqual("SELECT * FROM person;", select.to_str())

    def test_select_default_tempfiles(self):
        select = Select(None, "person")
        self.assertEqual("SELECT * FROM person TEMPFILES;", select.tempfiles().to_str())

    def test_select_default_by_id(self):
        select = Select(None, "person").by_id("john")
        self.assertEqual("SELECT * FROM person:john;", select.to_str())

    def test_select_star(self):
        select = Select(None, "person", "*")
        self.assertEqual("SELECT * FROM person;", str(select))

    def test_select_params(self):
        select = Select(None, "person", "name", "age")
        self.assertEqual("SELECT name, age FROM person;", str(select))

    def test_select_params_by_id(self):
        select = Select(None, "person", "name", "age").by_id("john")
        self.assertEqual("SELECT name, age FROM person:john;", str(select))

    def test_select_params_by_id_only(self):
        select = Select(None, "person", "name", "age").only().by_id("john")
        self.assertEqual("SELECT name, age FROM ONLY person:john;", str(select))
        self.assertEqual(["OK"], select.validate())

    def test_select_params_by_id_is_valid(self):
        select = Select(None, "person", "name", "age").only().by_id("john")
        self.assertEqual("SELECT name, age FROM ONLY person:john;", str(select))
        self.assertTrue(select.is_valid())

    def test_select_only(self):
        select = Select(None, "person").only()
        self.assertEqual("SELECT * FROM ONLY person;", str(select))
        self.assertEqual(["Expected a single result output when using the ONLY keyword"], select.validate())

    def test_value(self):
        select = Select(None, "person", value="age").by_id("john")
        self.assertEqual("SELECT VALUE age FROM person:john;", str(select))

    def test_value_cover_args(self):
        select = Select(None, "person", "name", "address", value="age").only().by_id("john")
        self.assertEqual("SELECT VALUE age FROM ONLY person:john;", str(select))

    def test_docs(self):
        """
        cover https://docs.surrealdb.com/docs/surrealql/statements/select#advanced-expressions
        """
        self.assertEqual("SELECT address.city FROM person;", Select(None, "person", "address.city").to_str())
        self.assertEqual("SELECT address.*.coordinates AS coordinates FROM person;",
                         Select(None, "person", alias=[("coordinates", "address.*.coordinates")]).to_str())
        self.assertEqual("SELECT address.coordinates[0] AS latitude FROM person;",
                         Select(None, "person", alias=[("latitude", "address.coordinates[0]")]).to_str())
        self.assertEqual("SELECT array::distinct(tags) FROM article;",
                         Select(None, "article", "array::distinct(tags)").to_str())
        self.assertEqual("SELECT ( ( celsius * 2 ) + 30 ) AS fahrenheit FROM temperature;",
                         Select(None, "temperature", alias=[("fahrenheit", "( ( celsius * 2 ) + 30 )")]).to_str())
        self.assertEqual("SELECT rating >= 4 AS positive FROM review;",
                         Select(None, "review", alias=[("positive", "rating >= 4")]).to_str())
        self.assertEqual("SELECT { weekly: false, monthly: true } AS `marketing settings` FROM user;",
                         Select(None, "user",
                                alias=[("`marketing settings`", "{ weekly: false, monthly: true }")]).to_str())
        self.assertEqual("SELECT address[WHERE active = true] FROM person;",
                         Select(None, "person", "address[WHERE active = true]").to_str())
        self.assertEqual("SELECT ->likes->friend.name AS friends FROM person:tobie;",
                         Select(None, "person", alias=[("friends", "->likes->friend.name")]).by_id("tobie").to_str())
        self.assertEqual("SELECT *, (SELECT * FROM events WHERE type = 'activity' LIMIT 5) AS history FROM user;",
                         Select(None, "user", "*",
                                alias=[("history", "(SELECT * FROM events WHERE type = 'activity' LIMIT 5)")]).to_str())
        self.assertEqual("SELECT * FROM person WHERE ->(reacted_to WHERE type='celebrate')->post;",
                         Select(None, "person").where("->(reacted_to WHERE type='celebrate')->post").to_str())
        self.assertEqual("SELECT array::group(tags) AS tags FROM article GROUP ALL;",
                         Select(None, "article", alias=[("tags", "array::group(tags)")]).group_all().to_str())

    def test_docs2(self):
        text = "SELECT * FROM person ORDER BY name TEMPFILES;"
        self.assertEqual(text, Select(None, "person").order_by("name").tempfiles().to_str())

    def test_omit(self):
        self.assertEqual("SELECT * OMIT password, opts.security FROM person;",
                         Select(None, "person").omit("password", "opts.security").to_str())

    def test_from_where(self):
        self.assertEqual("SELECT * FROM type::table($table) WHERE admin = true;",
                         Select(None, "type::table($table)").where("admin = true").to_str())

    def test_inner_query(self):
        first = Select(None, "user", alias=[("adult", "age >= 18")])
        second = Select(None, first).where("adult = true")
        self.assertEqual("SELECT * FROM (SELECT age >= 18 AS adult FROM user) WHERE adult = true;", second.to_str())

    def test_split(self):
        sel = Select(None, "user").split("emails")
        self.assertEqual("SELECT * FROM user SPLIT emails;", sel.to_str())

    def test_group_by(self):
        sel = Select(None, "user", "country").group_by("country")
        self.assertEqual("SELECT country FROM user GROUP BY country;", sel.to_str())

    def test_group_all(self):
        sel = Select(None, "person", alias=[("number_of_records", "count()")]).group_all()
        self.assertEqual("SELECT count() AS number_of_records FROM person GROUP ALL;", sel.to_str())

    def test_order_by_rand(self):
        sel = Select(None, "user").order_by_rand()
        self.assertEqual("SELECT * FROM user ORDER BY RAND();", sel.to_str())

    def test_order_by(self):
        sel = Select(None, "song").order_by("artist ASC", "rating DESC")
        self.assertEqual("SELECT * FROM song ORDER BY artist ASC, rating DESC;", sel.to_str())

    def test_limit(self):
        sel = Select(None, "person").limit(50)
        self.assertEqual("SELECT * FROM person LIMIT 50;", sel.to_str())

    def test_limit_start(self):
        sel = Select(None, "user").limit(50).start_at(50)
        self.assertEqual("SELECT * FROM user LIMIT 50 START 50;", sel.to_str())

    def test_fetch(self):
        sel = Select(None, "review", "*", "artist.email").fetch("artist")
        self.assertEqual("SELECT *, artist.email FROM review FETCH artist;", sel.to_str())

    def test_timeout(self):
        sel = Select(None, "person").where("->knows->person->(knows WHERE influencer = true)").timeout("5s")
        self.assertEqual("SELECT * FROM person WHERE ->knows->person->(knows WHERE influencer = true) TIMEOUT 5s;",
                         sel.to_str())

    def test_parallel(self):
        sel = Select(None, "person", "->purchased->product<-purchased<-person->purchased->product").by_id(
            "tobie").parallel()
        self.assertEqual(
            "SELECT ->purchased->product<-purchased<-person->purchased->product FROM person:tobie PARALLEL;",
            sel.to_str())

    def test_explain(self):
        sel = Select(None, "person").where("email='tobie@surrealdb.com'").explain()
        self.assertEqual("SELECT * FROM person WHERE email='tobie@surrealdb.com' EXPLAIN;", sel.to_str())
        sel = Select(None, "person").where("email='tobie@surrealdb.com'").explain_full()
        self.assertEqual("SELECT * FROM person WHERE email='tobie@surrealdb.com' EXPLAIN FULL;", sel.to_str())

    def test_only_limit(self):
        sel = Select(None, "table_name").only().limit(1)
        self.assertEqual("SELECT * FROM ONLY table_name LIMIT 1;", sel.to_str())

    def test_with_index(self):
        sel = Select(None, "person", "name").with_index("idx_name").where("job='engineer' AND genre = 'm'")
        self.assertEqual("SELECT name FROM person WITH INDEX idx_name WHERE job='engineer' AND genre = 'm';",
                         sel.to_str())

    def test_validate_recursive(self):
        result = Select(None, "user").only().limit(100).validate()
        self.assertEqual(['Expected a single result output when using the ONLY keyword'], result)
        result = Select(None, "user").limit(100).timeout("5h").validate()
        self.assertEqual(['Wrong duration 5h, allowed postfix are (ms, s, m)'], result)
        result = Select(None, "user").limit(100).timeout("5 m").validate()
        self.assertEqual(['Wrong duration format, should be like 5s'], result)
        result = Select(None, "user").only().limit(100).timeout("3d").validate()
        self.assertEqual(['Expected a single result output when using the ONLY keyword',
                          'Wrong duration 3d, allowed postfix are (ms, s, m)'], result)

    def test_and_or(self):
        sel = Select(None, "person", "name").with_index("idx_name").where("job='engineer'").AND("genre = 'm'")
        self.assertEqual("SELECT name FROM person WITH INDEX idx_name WHERE job='engineer' AND genre = 'm';",
                         sel.to_str())
        sel = Select(None, "person", "name").with_index("idx_name").where("job='engineer'").AND("genre = 'm'").OR(
            "x<100").limit(10)
        self.assertEqual(
            "SELECT name FROM person WITH INDEX idx_name WHERE job='engineer' AND genre = 'm' OR x<100 LIMIT 10;",
            sel.to_str())

    def test_sub_query(self):
        sub_query = Select(None, "events").where("type = 'active'")
        text = "SELECT * FROM (SELECT * FROM events WHERE type = 'active') LIMIT 5 PARALLEL;"
        self.assertEqual(text, Select(None, sub_query).limit(5).parallel().to_str())


if __name__ == '__main__':
    main()
