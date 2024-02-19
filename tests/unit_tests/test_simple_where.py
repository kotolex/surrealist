from unittest import TestCase

from surrealist.ql.statements.simple_statements import Where


class TestWhere(TestCase):
    def test_simple(self):
        self.assertEqual("WHERE admin = True", str(Where(admin=True)))

    def test_or(self):
        self.assertEqual("WHERE user = admin OR id = $admin.id", str(Where(user="admin").OR(id="$admin.id")))

    def test_and(self):
        self.assertEqual("WHERE user = admin AND id = $admin.id", str(Where(user="admin").AND(id="$admin.id")))
