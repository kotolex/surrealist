from unittest import TestCase

from surrealist.ql.statements.rebuild_index import RebuildIndex
from surrealist.ql.statements.simple_statements import Where


class TestSimple(TestCase):
    def test_simple(self):
        self.assertEqual("WHERE admin = true", str(Where(admin=True)))
        self.assertEqual("WHERE name='Alex', admin = true", str(Where("name='Alex'", admin=True)))
        self.assertEqual("WHERE name='Alex', admin = true", str(Where("name='Alex', admin = true")))
        self.assertEqual('WHERE name = "Alex", admin = true', str(Where(name='Alex', admin=True)))

    def test_or(self):
        self.assertEqual('WHERE user = "admin" OR id = "$admin.id"', str(Where(user="admin").OR(id="$admin.id")))
        self.assertEqual('WHERE user = "Alex" OR id = 1', str(Where(user="Alex").OR("id = 1")))
        self.assertEqual('WHERE user = "Alex" OR id = 1, inn = 100', str(Where(user="Alex").OR("id = 1", inn=100)))
        self.assertEqual('WHERE user = "Alex" OR id = 1', str(Where(user="Alex").OR(id=1)))

    def test_and(self):
        self.assertEqual('WHERE user = "admin" AND id = "$admin.id"', str(Where(user="admin").AND(id="$admin.id")))
        self.assertEqual('WHERE user = "Alex" AND id = 1', str(Where(user="Alex").AND("id = 1")))
        self.assertEqual('WHERE user = "Alex" AND id = 1, inn = 100', str(Where(user="Alex").AND("id = 1", inn=100)))
        self.assertEqual('WHERE user = "Alex" AND id = 1', str(Where(user="Alex").AND(id=1)))

    def test_rebuild(self):
        rb = RebuildIndex(None, "index_name", "table_name")
        self.assertEqual("REBUILD INDEX index_name ON TABLE table_name;", rb.to_str())
        rb = RebuildIndex(None, "index_name", "table_name", if_exists=True)
        self.assertEqual("REBUILD INDEX IF EXISTS index_name ON TABLE table_name;", rb.to_str())
