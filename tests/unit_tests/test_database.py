from unittest import TestCase, main

from surrealist import Where
from surrealist.ql.statements import Create, Update, Select
from surrealist.ql.statements.define import DefineEvent, DefineUser, DefineParam, DefineAnalyzer, DefineScope, \
    DefineIndex, DefineToken, DefineTable, DefineField
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

    def test_define_event_exists(self):
        text = "DEFINE EVENT IF NOT EXISTS email ON TABLE user WHEN $before.email != $after.email THEN (CREATE event " \
               "SET user = $value.id, time = time::now(), value = $after.email);"
        then = Create(None, "event").set("user = $value.id, time = time::now(), value = $after.email")
        self.assertEqual(text, DefineEvent(None, "email", table_name="user", then=then).if_not_exists().
                         when("$before.email != $after.email").to_str())

    def test_define_user(self):
        self.assertEqual("DEFINE USER john ON DATABASE PASSWORD '123456' ROLES OWNER;",
                         DefineUser(None, "john", "123456").role_owner().to_str())
        self.assertEqual("DEFINE USER john ON DATABASE PASSWORD '123456' ROLES VIEWER;",
                         DefineUser(None, "john", "123456").to_str())
        self.assertEqual("DEFINE USER john ON DATABASE PASSWORD '123456' ROLES EDITOR;",
                         DefineUser(None, "john", "123456").role_editor().to_str())

    def test_define_user_exists(self):
        self.assertEqual("DEFINE USER IF NOT EXISTS john ON DATABASE PASSWORD '123456' ROLES OWNER;",
                         DefineUser(None, "john", "123456").if_not_exists().role_owner().to_str())

    def test_define_param(self):
        self.assertEqual("DEFINE PARAM $user VALUE john;", DefineParam(None, "user", "john").to_str())

    def test_define_param_exists(self):
        self.assertEqual("DEFINE PARAM IF NOT EXISTS $user VALUE john;", DefineParam(None, "user", "john").
                         if_not_exists().to_str())

    def test_define_analyzer(self):
        self.assertEqual("DEFINE ANALYZER example_ascii TOKENIZERS class FILTERS ascii;",
                         DefineAnalyzer(None, "example_ascii").tokenizers("class").filters("ascii").to_str())

    def test_define_analyzer_exists(self):
        self.assertEqual("DEFINE ANALYZER IF NOT EXISTS example_ascii TOKENIZERS class FILTERS ascii;",
                         DefineAnalyzer(None, "example_ascii").if_not_exists().tokenizers("class").
                         filters("ascii").to_str())

    def test_define_scope(self):
        create = Create(None, "user").set("email = $email, pass = crypto::argon2::generate($pass)")
        select = Select(None, "user").where("email = $email AND crypto::argon2::compare(pass, $pass)")

        text = """DEFINE SCOPE account SESSION 24h 
SIGNUP (CREATE user SET email = $email, pass = crypto::argon2::generate($pass)) 
SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass));"""
        self.assertEqual(text, DefineScope(None, "account", "24h", signup=create, signin=select).to_str())

    def test_define_scope_exists(self):
        create = Create(None, "user").set("email = $email, pass = crypto::argon2::generate($pass)")
        select = Select(None, "user").where("email = $email AND crypto::argon2::compare(pass, $pass)")

        text = """DEFINE SCOPE IF NOT EXISTS account SESSION 24h 
SIGNUP (CREATE user SET email = $email, pass = crypto::argon2::generate($pass)) 
SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass));"""
        self.assertEqual(text, DefineScope(None, "account", "24h", signup=create, signin=select).if_not_exists().
                         to_str())

    def test_define_index(self):
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii BM25 HIGHLIGHTS;"
        self.assertEqual(text,
                         DefineIndex(None, "userNameIndex", "user").columns("name").search_analyzer("ascii").to_str())

    def test_define_index_mtree(self):
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name MTREE DIMENSION 4;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").mtree(4).to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name MTREE DIMENSION 4 DIST EUCLIDEAN;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").mtree(4).distance_euclidean().to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name MTREE DIMENSION 4 DIST MANHATTAN;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").mtree(4).distance_manhattan().to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name MTREE DIMENSION 4 DIST MINKOWSKI;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").mtree(4).distance_minkowski().to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name MTREE DIMENSION 4 DIST COSINE;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").mtree(4).distance_cosine().to_str())

    def test_define_index_hnsw(self):
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name HNSW DIMENSION 4;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").hnsw(4).to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name HNSW DIMENSION 4 DIST EUCLIDEAN;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").hnsw(4).distance_euclidean().to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name HNSW DIMENSION 4 DIST MANHATTAN;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").hnsw(4).distance_manhattan().to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name HNSW DIMENSION 4 DIST MINKOWSKI;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").hnsw(4).distance_minkowski().to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name HNSW DIMENSION 4 DIST COSINE;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").hnsw(4).distance_cosine().to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name HNSW DIMENSION 4 DIST COSINE EFC 150 M 2;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").hnsw(4).distance_cosine().efc("150").m(2).to_str())

    def test_define_index_exists(self):
        text = "DEFINE INDEX IF NOT EXISTS userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii " \
               "BM25 HIGHLIGHTS;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").if_not_exists().columns("name").
                         search_analyzer("ascii").to_str())

    def test_define_token(self):
        text = """DEFINE TOKEN token_name ON DATABASE 
TYPE RS256 
VALUE "value";"""
        self.assertEqual(text, DefineToken(None, "token_name", "RS256", "value").to_str())

    def test_define_token_exists(self):
        text = """DEFINE TOKEN IF NOT EXISTS token_name ON DATABASE 
TYPE RS256 
VALUE "value";"""
        self.assertEqual(text, DefineToken(None, "token_name", "RS256", "value").if_not_exists().to_str())

    def test_define_table(self):
        text = "DEFINE TABLE table_name;"
        self.assertEqual(text, DefineTable(None, "table_name").to_str())

        text = "DEFINE TABLE table_name DROP CHANGEFEED 1s;"
        self.assertEqual(text, DefineTable(None, "table_name").drop().changefeed("1s").to_str())

        text = "DEFINE TABLE table_name DROP CHANGEFEED 1s INCLUDE ORIGINAL;"
        self.assertEqual(text, DefineTable(None, "table_name").drop().changefeed("1s", include_original=True).to_str())

        text = "DEFINE TABLE table_name SCHEMALESS PERMISSIONS NONE;"
        self.assertEqual(text, DefineTable(None, "table_name").schemaless().permissions_none().to_str())

        text = "DEFINE TABLE table_name SCHEMAFULL PERMISSIONS FULL;"
        self.assertEqual(text, DefineTable(None, "table_name").schemafull().permissions_full().to_str())

        text = """DEFINE TABLE post 
PERMISSIONS 
 FOR select WHERE published = true OR user = $auth.id
 FOR create, update WHERE user = $auth.id
 FOR delete WHERE user = $auth.id OR $auth.admin = true;"""
        select = Where(published=True).OR('user = $auth.id')
        create = Where('user = $auth.id')
        delete = Where('user = $auth.id').OR("$auth.admin = true")
        self.assertEqual(text, DefineTable(None, "post").permissions_for(select=select, create=create,
                                                                         update=create, delete=delete).to_str())
        text = """DEFINE TABLE temperatures_by_month AS
 SELECT count() AS total, time::month(recorded_at) AS month FROM reading GROUP BY city
;"""
        select = Select(None, "reading", alias=[("total", "count()"), ("month", "time::month(recorded_at)")]).group_by(
            "city")
        self.assertEqual(text, DefineTable(None, "temperatures_by_month").alias(select).to_str())

    def test_define_table_exists(self):
        text = "DEFINE TABLE IF NOT EXISTS table_name;"
        self.assertEqual(text, DefineTable(None, "table_name").if_not_exists().to_str())

    def test_define_field(self):
        text = "DEFINE FIELD new_field ON TABLE some_table;"
        self.assertEqual(text, DefineField(None, "new_field", "some_table").to_str())
        text = "DEFINE FIELD field ON TABLE user TYPE string;"
        self.assertEqual(text, DefineField(None, "field", "user").type("string").to_str())
        text = "DEFINE FIELD field ON TABLE user FLEXIBLE TYPE bool;"
        self.assertEqual(text, DefineField(None, "field", "user").type("bool", is_flexible=True).to_str())
        text = "DEFINE FIELD locked ON TABLE user TYPE bool DEFAULT false;"
        self.assertEqual(text, DefineField(None, "locked", "user").type("bool").default("false").to_str())
        text = "DEFINE FIELD updated ON TABLE resource DEFAULT time::now();"
        self.assertEqual(text, DefineField(None, "updated", "resource").default("time::now()").to_str())
        text = "DEFINE FIELD updated ON TABLE resource DEFAULT time::now() READONLY;"
        self.assertEqual(text, DefineField(None, "updated", "resource").default("time::now()").read_only().to_str())
        text = "DEFINE FIELD email ON TABLE resource TYPE string ASSERT string::is::email($value);"
        self.assertEqual(text, DefineField(None, "email", "resource").type("string").asserts(
            "string::is::email($value)").to_str())
        text = "DEFINE FIELD comment ON TABLE resource FLEXIBLE TYPE string PERMISSIONS FULL;"
        self.assertEqual(text, DefineField(None, "comment", "resource").type("string",
                                                                             is_flexible=True).permissions_full().to_str())

    def test_define_field_exists(self):
        text = "DEFINE FIELD IF NOT EXISTS new_field ON TABLE some_table;"
        self.assertEqual(text, DefineField(None, "new_field", "some_table").if_not_exists().to_str())

    def test_define_table_type(self):
        self.assertEqual(DefineTable(None, "person").type_any().to_str(), "DEFINE TABLE person TYPE ANY;")
        self.assertEqual(DefineTable(None, "person").type_normal().to_str(), "DEFINE TABLE person TYPE NORMAL;")
        self.assertEqual(DefineTable(None, "likes").type_relation().to_str(), "DEFINE TABLE likes TYPE RELATION;")

        text = "DEFINE TABLE likes TYPE RELATION FROM user TO post;"
        self.assertEqual(DefineTable(None, "likes").type_relation(from_to=("user", "post")).to_str(), text)
        text = "DEFINE TABLE likes TYPE RELATION IN user OUT post;"
        self.assertEqual(DefineTable(None, "likes").type_relation(from_to=("user", "post"), use_from_to=False).to_str(),
                         text)


if __name__ == '__main__':
    main()
