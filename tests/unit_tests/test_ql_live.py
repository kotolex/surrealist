from unittest import TestCase, main

from surrealist.ql.statements.live import Live


class TestLive(TestCase):
    def test_default(self):
        self.assertEqual("LIVE SELECT * FROM person;", Live(None, "person").to_str())

    def test_diff_full(self):
        self.assertEqual("LIVE SELECT DIFF FROM person WHERE x>10 FETCH id;",
                         Live(None, "person", use_diff=True).where("x>10").fetch("id").to_str())

    def test_alias_full(self):
        self.assertEqual("LIVE SELECT author.id AS auth FROM person WHERE x > 10 FETCH id;",
                         Live(None, "person").alias("author.id","auth").where("x > 10").fetch("id").to_str())


if __name__ == '__main__':
    main()