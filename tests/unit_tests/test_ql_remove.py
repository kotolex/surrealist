from unittest import TestCase, main

from surrealist.ql.statements.remove import Remove


class TestRemove(TestCase):
    def test_default(self):
        self.assertEqual("REMOVE TABLE person;", Remove(None, "person").to_str())

    def test_param(self):
        self.assertEqual("REMOVE PARAM $salt;", Remove(None, "person", type_="PARAM", name="salt").to_str())

    def test_field(self):
        self.assertEqual("REMOVE FIELD salt ON TABLE person;",
                         Remove(None, "person", type_="FIELD", name="salt").to_str())

    def test_event(self):
        self.assertEqual("REMOVE EVENT salt ON TABLE person;",
                         Remove(None, "person", type_="EVENT", name="salt").to_str())

    def test_index(self):
        self.assertEqual("REMOVE INDEX salt ON TABLE person;",
                         Remove(None, "person", type_="INDEX", name="salt").to_str())

    def test_wrong_type_failed(self):
        with self.assertRaises(ValueError):
            Remove(None, "person", type_="WRONG", name="salt")


if __name__ == '__main__':
    main()
