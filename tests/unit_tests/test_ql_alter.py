from unittest import TestCase, main

from surrealist.ql.statements.alter import Alter
from surrealist.ql.statements.select import Select


class TestAlter(TestCase):
    def test_simple(self):
        alter = Alter(None, "user")
        self.assertEqual(alter.to_str(), "ALTER TABLE user;")

    def test_simple_comment(self):
        alter = Alter(None, "user").comment("comment")
        self.assertEqual(alter.to_str(), 'ALTER TABLE user COMMENT "comment";')

    def test_drop(self):
        alter = Alter(None, "user").drop()
        self.assertEqual(alter.to_str(), "ALTER TABLE user DROP;")

    def test_if_exists(self):
        alter = Alter(None, "user").if_not_exists()
        self.assertEqual(alter.to_str(), "ALTER TABLE IF NOT EXISTS user;")

    def test_schemafull(self):
        alter = Alter(None, "user").schema_full()
        self.assertEqual(alter.to_str(), "ALTER TABLE user SCHEMAFULL;")

    def test_schemaless(self):
        alter = Alter(None, "user").schema_less()
        self.assertEqual(alter.to_str(), "ALTER TABLE user SCHEMALESS;")

    def test_perm_full_comment(self):
        alter = Alter(None, "user").permissions_full().comment("comment")
        self.assertEqual(alter.to_str(), 'ALTER TABLE user PERMISSIONS FULL COMMENT "comment";')

    def test_raw_select(self):
        alter = Alter(None, "user").as_raw_select("name", "users", "age > 10", "country")
        self.assertEqual(alter.to_str(), 'ALTER TABLE user AS SELECT name FROM users WHERE age > 10 GROUP BY country;')

    def test_as_select(self):
        select = Select(None, "users", "name").where("age > 10").group_by("country")
        alter = Alter(None, "user").as_select(select)
        self.assertEqual(alter.to_str(), 'ALTER TABLE user AS SELECT name FROM users WHERE age > 10 GROUP BY country;')


if __name__ == '__main__':
    main()
