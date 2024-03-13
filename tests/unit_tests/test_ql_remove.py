from unittest import TestCase, main

from surrealist.ql.statements.remove import Remove


class TestRemove(TestCase):
    def test_default(self):
        self.assertEqual("REMOVE TABLE person;", Remove(None, "person").to_str())
        self.assertEqual("REMOVE TABLE IF EXISTS person;", Remove(None, "person").if_exists().to_str())

    def test_param(self):
        self.assertEqual("REMOVE PARAM $salt;", Remove(None, "person", type_="PARAM", name="salt").to_str())
        self.assertEqual("REMOVE PARAM IF EXISTS $salt;",
                         Remove(None, "person", type_="PARAM", name="salt").if_exists().to_str())

    def test_field(self):
        self.assertEqual("REMOVE FIELD salt ON TABLE person;",
                         Remove(None, "person", type_="FIELD", name="salt").to_str())
        self.assertEqual("REMOVE FIELD IF EXISTS salt ON TABLE person;",
                         Remove(None, "person", type_="FIELD", name="salt").if_exists().to_str())

    def test_event(self):
        self.assertEqual("REMOVE EVENT salt ON TABLE person;",
                         Remove(None, "person", type_="EVENT", name="salt").to_str())
        self.assertEqual("REMOVE EVENT IF EXISTS salt ON TABLE person;",
                         Remove(None, "person", type_="EVENT", name="salt").if_exists().to_str())

    def test_index(self):
        self.assertEqual("REMOVE INDEX salt ON TABLE person;",
                         Remove(None, "person", type_="INDEX", name="salt").to_str())
        self.assertEqual("REMOVE INDEX IF EXISTS salt ON TABLE person;",
                         Remove(None, "person", type_="INDEX", name="salt").if_exists().to_str())

    def test_token(self):
        self.assertEqual("REMOVE TOKEN token ON DATABASE;",
                         Remove(None, "", type_="TOKEN", name="token").to_str())
        self.assertEqual("REMOVE TOKEN IF EXISTS token ON DATABASE;",
                         Remove(None, "", type_="TOKEN", name="token").if_exists().to_str())

    def test_user(self):
        self.assertEqual("REMOVE USER user ON DATABASE;",
                         Remove(None, "", type_="USER", name="user").to_str())
        self.assertEqual("REMOVE USER IF EXISTS user ON DATABASE;",
                         Remove(None, "", type_="USER", name="user").if_exists().to_str())

    def test_analyzer(self):
        self.assertEqual("REMOVE ANALYZER name;",
                         Remove(None, "", type_="ANALYZER", name="name").to_str())
        self.assertEqual("REMOVE ANALYZER IF EXISTS name;",
                         Remove(None, "", type_="ANALYZER", name="name").if_exists().to_str())

    def test_scope(self):
        self.assertEqual("REMOVE SCOPE name;",
                         Remove(None, "", type_="SCOPE", name="name").to_str())
        self.assertEqual("REMOVE SCOPE IF EXISTS name;",
                         Remove(None, "", type_="SCOPE", name="name").if_exists().to_str())

    def test_wrong_type_failed(self):
        with self.assertRaises(ValueError):
            Remove(None, "person", type_="WRONG", name="salt")


if __name__ == '__main__':
    main()
