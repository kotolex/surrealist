from unittest import TestCase, main

from surrealist.ql.statements import Create, Update, Select
from surrealist.ql.statements.define import DefineEvent, DefineUser, DefineParam, DefineAnalyzer, DefineScope, \
    DefineIndex, DefineToken
from surrealist.ql.statements.transaction import Transaction

text = """BEGIN TRANSACTION;

CREATE author CONTENT {"name": "john", "id": "john"};
CREATE book CONTENT {"title": "Title", "author": "author:john"};
UPDATE counter:author_count SET count +=1;

COMMIT TRANSACTION;"""


class TestDatabase(TestCase):
    def test_tr(self):
        create_author = Create(None, "author").content({"name": "john", "id": "john"})
        create_book = Create(None, "book").content({"title": "Title", "author": "author:john"})
        counter_inc = Update(None, "counter", "author_count").set("count +=1")

        transaction = Transaction(None, [create_author, create_book, counter_inc])
        self.assertEqual(text, transaction.to_str())

    def test_define_event(self):
        text = "DEFINE EVENT email ON TABLE user WHEN $before.email != $after.email THEN (CREATE event SET " \
               "user = $value.id, time = time::now(), value = $after.email);"
        then = Create(None, "event").set("user = $value.id, time = time::now(), value = $after.email")
        self.assertEqual(text, DefineEvent(None, "email", table_name="user", then=then).
                         when("$before.email != $after.email").to_str())

    def test_define_user(self):
        self.assertEqual("DEFINE USER john ON DATABASE PASSWORD '123456' ROLES OWNER;",
                         DefineUser(None, "john", "123456").role_owner().to_str())
        self.assertEqual("DEFINE USER john ON DATABASE PASSWORD '123456' ROLES VIEWER;",
                         DefineUser(None, "john", "123456").to_str())
        self.assertEqual("DEFINE USER john ON DATABASE PASSWORD '123456' ROLES EDITOR;",
                         DefineUser(None, "john", "123456").role_editor().to_str())

    def test_define_param(self):
        self.assertEqual("DEFINE PARAM $user VALUE john;", DefineParam(None, "user", "john").to_str())

    def test_define_analyzer(self):
        self.assertEqual("DEFINE ANALYZER example_ascii TOKENIZERS class FILTERS ascii;",
                         DefineAnalyzer(None, "example_ascii").tokenizers("class").filters("ascii").to_str())

    def test_define_scope(self):
        create = Create(None, "user").set("email = $email, pass = crypto::argon2::generate($pass)")
        select = Select(None, "user").where("email = $email AND crypto::argon2::compare(pass, $pass)")

        text = """DEFINE SCOPE account SESSION 24h 
SIGNUP (CREATE user SET email = $email, pass = crypto::argon2::generate($pass)) 
SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass));"""
        self.assertEqual(text, DefineScope(None, "account", "24h", signup=create, signin=select).to_str())

    def test_define_index(self):
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii BM25 HIGHLIGHTS;"
        self.assertEqual(text,
                         DefineIndex(None, "userNameIndex", "user").columns("name").search_analyzer("ascii").to_str())

    def test_define_token(self):
        text ="""DEFINE TOKEN token_name ON DATABASE 
TYPE RS256 
VALUE "value";"""
        self.assertEqual(text, DefineToken(None, "token_name", "RS256", "value").to_str())


if __name__ == '__main__':
    main()
