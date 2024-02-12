from unittest import TestCase, main

from surrealist.ql.statements.relate import Relate


class TestCreate(TestCase):
    def test_relate_default(self):
        create = Relate(None, "person")
        self.assertEqual("RELATE person;", create.to_str())

    def test_content(self):
        text = 'RELATE person:john->wrote->article:main CONTENT {"time": {"written": "time::now()"}};'
        data = {"time": {"written": "time::now()"}}
        self.assertEqual(text, Relate(None, "person:john->wrote->article:main").content(data).to_str())

    def test_set(self):
        text = 'RELATE person:john->wrote->article:main SET time.written = time::now();'
        self.assertEqual(text, Relate(None, "person:john->wrote->article:main").set("time.written = time::now()").to_str())

    def test_returns(self):
        text = 'RELATE person:john->wrote->article:main RETURN DIFF;'
        self.assertEqual(text, Relate(None, "person:john->wrote->article:main").return_diff().to_str())
        text = 'RELATE person:john->wrote->article:main RETURN AFTER;'
        self.assertEqual(text, Relate(None, "person:john->wrote->article:main").return_after().to_str())
        text = 'RELATE person:john->wrote->article:main RETURN BEFORE;'
        self.assertEqual(text, Relate(None, "person:john->wrote->article:main").return_before().to_str())
        text = 'RELATE person:john->wrote->article:main RETURN NONE;'
        self.assertEqual(text, Relate(None, "person:john->wrote->article:main").return_none().to_str())
        text = 'RELATE person:john->wrote->article:main RETURN time;'
        self.assertEqual(text, Relate(None, "person:john->wrote->article:main").returns("time").to_str())


if __name__ == '__main__':
    main()