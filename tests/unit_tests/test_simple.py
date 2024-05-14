from unittest import TestCase

from surrealist.ql.statements.simple_statements import Where
from surrealist.ql.statements.rebuild_index import RebuildIndex


class TestSimple(TestCase):
    def test_simple(self):
        self.assertEqual("WHERE admin = True", str(Where(admin=True)))

    def test_or(self):
        self.assertEqual("WHERE user = admin OR id = $admin.id", str(Where(user="admin").OR(id="$admin.id")))

    def test_and(self):
        self.assertEqual("WHERE user = admin AND id = $admin.id", str(Where(user="admin").AND(id="$admin.id")))

    def test_rebuild(self):
        rb = RebuildIndex(None, "index_name", "table_name")
        self.assertEqual("REBUILD INDEX index_name ON TABLE table_name;", rb.to_str())
        rb = RebuildIndex(None, "index_name", "table_name", if_exists=True)
        self.assertEqual("REBUILD INDEX IF EXISTS index_name ON TABLE table_name;", rb.to_str())
