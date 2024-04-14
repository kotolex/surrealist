from unittest import TestCase, main

from surrealist.ql.statements.live import Live


class TestLive(TestCase):
    def test_default(self):
        self.assertEqual("LIVE SELECT * FROM person;", Live(None, "person", print).to_str())

    def test_diff_full(self):
        self.assertEqual("LIVE SELECT DIFF FROM person WHERE x>10 FETCH id;",
                         Live(None, "person", use_diff=True, callback=print).where("x>10").fetch("id").to_str())

    def test_value(self):
        self.assertEqual("LIVE SELECT VALUE name FROM person;",
                         Live(None, "person", callback=print).value("name").to_str())

    def test_select(self):
        self.assertEqual("LIVE SELECT *, name as author FROM person;",
                         Live(None, "person", callback=print, select="*, name as author").to_str())

    def test_alias_full(self):
        self.assertEqual("LIVE SELECT author.id AS auth FROM person WHERE x > 10 FETCH id;",
                         Live(None, "person", print).alias("author.id", "auth").where("x > 10").fetch("id").to_str())

    def test_validate(self):
        self.assertEqual(['If select is provided, value, diff and alias parameters will be ignored'],
                         Live(None, "person", callback=print, select="*, name as author").value("a").validate())
        self.assertEqual(['If select is provided, value, diff and alias parameters will be ignored'],
                         Live(None, "person", callback=print, select="*, name as author", use_diff=True).validate())
        self.assertEqual(['If select is provided, value, diff and alias parameters will be ignored'],
                         Live(None, "person", callback=print, select="*, name as author").alias("a.id", "a").validate())


if __name__ == '__main__':
    main()
