from unittest import TestCase, main

from surrealist import Where, Algorithm
from surrealist.ql.statements import Create, Update, Select
from surrealist.ql.statements.define import DefineEvent, DefineParam, DefineScope, \
    DefineIndex, DefineToken, DefineTable, DefineField
from surrealist.ql.statements.define_analyzer import DefineAnalyzer
from surrealist.ql.statements.define_user import DefineUser
from surrealist.ql.statements.define_access import DefineAccessJwt, DefineAccessRecord
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

    def test_define_event_comment(self):
        text = "DEFINE EVENT email ON TABLE user WHEN $before.email != $after.email THEN (CREATE event SET " \
               "user = $value.id, time = time::now(), value = $after.email) COMMENT \"some\";"
        then = Create(None, "event").set("user = $value.id, time = time::now(), value = $after.email")
        self.assertEqual(text, DefineEvent(None, "email", table_name="user", then=then).
                         when("$before.email != $after.email").comment("some").to_str())

    def test_define_event_exists(self):
        text = "DEFINE EVENT IF NOT EXISTS email ON TABLE user WHEN $before.email != $after.email THEN (CREATE event " \
               "SET user = $value.id, time = time::now(), value = $after.email);"
        then = Create(None, "event").set("user = $value.id, time = time::now(), value = $after.email")
        self.assertEqual(text, DefineEvent(None, "email", table_name="user", then=then).if_not_exists().
                         when("$before.email != $after.email").to_str())

    def test_define_event_over(self):
        text = "DEFINE EVENT OVERWRITE email ON TABLE user WHEN $before.email != $after.email THEN (CREATE event " \
               "SET user = $value.id, time = time::now(), value = $after.email);"
        then = Create(None, "event").set("user = $value.id, time = time::now(), value = $after.email")
        self.assertEqual(text, DefineEvent(None, "email", table_name="user", then=then).overwrite().
                         when("$before.email != $after.email").to_str())

    def test_define_user(self):
        self.assertEqual("DEFINE USER john ON DATABASE PASSWORD '123456' ROLES OWNER;",
                         DefineUser(None, "john").password("123456").role_owner().to_str())
        self.assertEqual("DEFINE USER john ON DATABASE PASSWORD '123456';",
                         DefineUser(None, "john").password("123456").to_str())
        self.assertEqual("DEFINE USER john ON DATABASE PASSWORD '123456' ROLES EDITOR;",
                         DefineUser(None, "john").password("123456").role_editor().to_str())

    def test_define_user_exists(self):
        self.assertEqual("DEFINE USER IF NOT EXISTS john ON DATABASE PASSWORD '123456' ROLES OWNER;",
                         DefineUser(None, "john").password("123456").if_not_exists().role_owner().to_str())

    def test_define_user_over(self):
        self.assertEqual("DEFINE USER OVERWRITE john ON DATABASE PASSWORD '123456' ROLES OWNER;",
                         DefineUser(None, "john").password("123456").overwrite().role_owner().to_str())

    def test_define_user_comment(self):
        self.assertEqual("DEFINE USER john ON DATABASE PASSWORD '123456' ROLES OWNER COMMENT \"some\";",
                         DefineUser(None, "john").password("123456").role_owner().comment("some").to_str())

    def test_define_param(self):
        self.assertEqual("DEFINE PARAM $user VALUE john;", DefineParam(None, "user", "john").to_str())

    def test_define_param_comment(self):
        self.assertEqual("DEFINE PARAM $user VALUE john COMMENT \"some\";",
                         DefineParam(None, "user", "john").comment("some").to_str())

    def test_define_param_exists(self):
        self.assertEqual("DEFINE PARAM IF NOT EXISTS $user VALUE john;", DefineParam(None, "user", "john").
                         if_not_exists().to_str())

    def test_define_param_over(self):
        self.assertEqual("DEFINE PARAM OVERWRITE $user VALUE john;", DefineParam(None, "user", "john").
                         overwrite().to_str())

    def test_define_analyzer(self):
        self.assertEqual("DEFINE ANALYZER example_ascii TOKENIZERS class FILTERS ascii;",
                         DefineAnalyzer(None, "example_ascii").tokenizer_class().filter_ascii().to_str())
        self.assertEqual("DEFINE ANALYZER example_ascii TOKENIZERS camel, class FILTERS ascii, ngram(1,5);",
                         DefineAnalyzer(None, "example_ascii").tokenizer_class().tokenizer_camel().filter_ascii().filter_ngram(1,5).to_str())

    def test_define_analyzer_comment(self):
        self.assertEqual("DEFINE ANALYZER example_ascii TOKENIZERS class FILTERS ascii COMMENT \"some\";",
                         DefineAnalyzer(None, "example_ascii").tokenizer_class().filter_ascii().comment("some").to_str())

    def test_define_analyzer_exists(self):
        self.assertEqual("DEFINE ANALYZER IF NOT EXISTS example_ascii TOKENIZERS class FILTERS lowercase;",
                         DefineAnalyzer(None, "example_ascii").if_not_exists().tokenizer_class().
                         filter_lowercase().to_str())

    def test_define_analyzer_over(self):
        self.assertEqual("DEFINE ANALYZER OVERWRITE example_ascii TOKENIZERS class FILTERS lowercase;",
                         DefineAnalyzer(None, "example_ascii").overwrite().tokenizer_class().
                         filter_lowercase().to_str())

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
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii;"
        self.assertEqual(text,
                         DefineIndex(None, "userNameIndex", "user").columns("name").search_analyzer("ascii").to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii BM25 HIGHLIGHTS;"
        self.assertEqual(text,
                         DefineIndex(None, "userNameIndex", "user").columns("name").search_analyzer("ascii").bm25().highlights().to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii BM25 1.2 0.7;"
        self.assertEqual(text,
                         DefineIndex(None, "userNameIndex", "user").columns("name").search_analyzer(
                             "ascii").bm25(1.2, 0.7).to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii BM25 1.2 0.0;"
        self.assertEqual(text,
                         DefineIndex(None, "userNameIndex", "user").columns("name").search_analyzer(
                             "ascii").bm25(1.2).to_str())
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii BM25 0.0 1.2;"
        self.assertEqual(text,
                         DefineIndex(None, "userNameIndex", "user").columns("name").search_analyzer(
                             "ascii").bm25(None, 1.2).to_str())

    def test_define_index_comment(self):
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii COMMENT \"some\";"
        self.assertEqual(text,
                         DefineIndex(None, "userNameIndex", "user").columns("name").search_analyzer("ascii").comment("some").to_str())

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
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name MTREE DIMENSION 3 TYPE F64 CAPACITY 50;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").mtree(3).type_f64().capacity(50).to_str())

    def test_define_index_mtree_comment(self):
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name MTREE DIMENSION 3 TYPE F64 CAPACITY 50 COMMENT \"some\";"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").mtree(3).type_f64().capacity(50).comment("some").to_str())

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
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").hnsw(4).distance_cosine().efc(150).max_connections(2).to_str())

    def test_define_index_hnsw_comment(self):
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name HNSW DIMENSION 4 DIST COSINE COMMENT \"some\";"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").hnsw(4).distance_cosine().comment("some").to_str())

    def test_define_index_hnsw_concurrently(self):
        text = "DEFINE INDEX userNameIndex ON TABLE user COLUMNS name HNSW DIMENSION 4 DIST COSINE CONCURRENTLY;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").columns("name").hnsw(4).distance_cosine().concurrently().to_str())

    def test_define_index_exists(self):
        text = "DEFINE INDEX IF NOT EXISTS userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").if_not_exists().columns("name").
                         search_analyzer("ascii").to_str())

    def test_define_index_over(self):
        text = "DEFINE INDEX OVERWRITE userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii;"
        self.assertEqual(text, DefineIndex(None, "userNameIndex", "user").overwrite().columns("name").
                         search_analyzer("ascii").to_str())

    def test_define_token(self):
        text = """DEFINE TOKEN token_name ON DATABASE 
TYPE RS256 
VALUE "value";"""
        self.assertEqual(text, DefineToken(None, "token_name", Algorithm.RS256, "value").to_str())

    def test_define_token_exists(self):
        text = """DEFINE TOKEN IF NOT EXISTS token_name ON DATABASE 
TYPE RS256 
VALUE "value";"""
        self.assertEqual(text, DefineToken(None, "token_name", Algorithm.RS256, "value").if_not_exists().to_str())

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

    def test_define_table_comment(self):
        text = "DEFINE TABLE table_name COMMENT \"some\";"
        self.assertEqual(text, DefineTable(None, "table_name").comment("some").to_str())

    def test_define_table_exists(self):
        text = "DEFINE TABLE IF NOT EXISTS table_name;"
        self.assertEqual(text, DefineTable(None, "table_name").if_not_exists().to_str())

    def test_define_table_over(self):
        text = "DEFINE TABLE OVERWRITE table_name;"
        self.assertEqual(text, DefineTable(None, "table_name").overwrite().to_str())

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

    def test_define_field_comment(self):
        text = "DEFINE FIELD updated ON TABLE resource DEFAULT time::now() COMMENT \"some\";"
        self.assertEqual(text, DefineField(None, "updated", "resource").default("time::now()").comment("some").to_str())

    def test_define_field_exists(self):
        text = "DEFINE FIELD IF NOT EXISTS new_field ON TABLE some_table;"
        self.assertEqual(text, DefineField(None, "new_field", "some_table").if_not_exists().to_str())

    def test_define_field_over(self):
        text = "DEFINE FIELD OVERWRITE new_field ON TABLE some_table;"
        self.assertEqual(text, DefineField(None, "new_field", "some_table").overwrite().to_str())

    def test_define_table_type(self):
        self.assertEqual(DefineTable(None, "person").type_any().to_str(), "DEFINE TABLE person TYPE ANY;")
        self.assertEqual(DefineTable(None, "person").type_normal().to_str(), "DEFINE TABLE person TYPE NORMAL;")
        self.assertEqual(DefineTable(None, "likes").type_relation().to_str(), "DEFINE TABLE likes TYPE RELATION;")

        text = "DEFINE TABLE likes TYPE RELATION FROM user TO post;"
        self.assertEqual(DefineTable(None, "likes").type_relation(from_to=("user", "post")).to_str(), text)
        text = "DEFINE TABLE likes TYPE RELATION IN user OUT post;"
        self.assertEqual(DefineTable(None, "likes").type_relation(from_to=("user", "post"), use_from_to=False).to_str(),
                         text)

    def test_define_table_changefeed_validate(self):
        self.assertEqual(DefineTable(None, "person").changefeed("1s").validate(), ['OK'])
        self.assertEqual(DefineTable(None, "person").changefeed("1r").validate(), ["Wrong duration 1r, allowed postfix are ('w', 'y', 'd', 'h', 'ms', 's', 'm')"])

    def test_define_access_jwt(self):
        key = "sNSYneezcr8kqphfOC6NwwraUHJCVAt"
        text = "DEFINE ACCESS token_name ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'sNSYneezcr8kqphfOC6NwwraUHJCVAt';"
        self.assertEqual(text, DefineAccessJwt(None, "token_name").algorithm(Algorithm.HS512, key).to_str())
        text = "DEFINE ACCESS token_name ON DATABASE TYPE JWT URL 'http://example.com';"
        self.assertEqual(text, DefineAccessJwt(None, "token_name").url("http://example.com").to_str())
        text = "DEFINE ACCESS token_name ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'sNSYneezcr8kqphfOC6NwwraUHJCVAt' DURATION FOR SESSION 1h;"
        self.assertEqual(text, DefineAccessJwt(None, "token_name").algorithm(Algorithm.HS512, key).duration("1h").to_str())

    def test_define_access_jwt_exists_over(self):
        text = "DEFINE ACCESS IF NOT EXISTS token_name ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';"
        self.assertEqual(text, DefineAccessJwt(None, "token_name").algorithm(Algorithm.HS512, "secret").if_not_exists().to_str())
        text = "DEFINE ACCESS OVERWRITE token_name ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret';"
        self.assertEqual(text, DefineAccessJwt(None, "token_name").algorithm(Algorithm.HS512,
                                                                             "secret").overwrite().to_str())


    def test_define_access_record(self):
        # Examples for DEFINE ACCESS ... RECORD
        create = Create(None, "user").set(email="$email", passw="crypto::argon2::generate($pass)")
        select = Select(None, "user").where("email = $email AND crypto::argon2::compare(pass, $pass)")

        text = """DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNIN SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) SIGNUP CREATE user SET email = "$email", passw = "crypto::argon2::generate($pass)" DURATION FOR TOKEN 15m, DURATION FOR SESSION 12h;"""
        self.assertEqual(text, DefineAccessRecord(None, "account").signup(create).signin(select).duration_for_token(
            "15m").duration_for_session("12h").to_str())

        text = "DEFINE ACCESS token ON DATABASE TYPE RECORD ALGORITHM HS256 KEY 'some_key';"
        self.assertEqual(text, DefineAccessRecord(None, "token").algorithm(Algorithm.HS256, "some_key").to_str())

        text= "DEFINE ACCESS token ON DATABASE TYPE RECORD ALGORITHM RS256 KEY 'some_key' WITH ISSUER KEY 'issuer_key';"
        self.assertEqual(text, DefineAccessRecord(None, "token").algorithm(Algorithm.RS256, "some_key", issuer_key="issuer_key").to_str())

        raw = """{
        RETURN IF $auth.id {
            RETURN $auth.id;
        } ELSE IF $token.email {
            RETURN SELECT * FROM user WHERE email = $token.email;
        };
    }"""
        text = """DEFINE ACCESS user ON DATABASE TYPE RECORD AUTHENTICATE {
        RETURN IF $auth.id {
            RETURN $auth.id;
        } ELSE IF $token.email {
            RETURN SELECT * FROM user WHERE email = $token.email;
        };
    } ALGORITHM HS512 KEY 'secret';"""
        self.assertEqual(text, DefineAccessRecord(None, "user").authenticate(raw).algorithm(Algorithm.HS512, "secret").to_str())

    def test_define_access_rec_exists_over(self):
        text = "DEFINE ACCESS IF NOT EXISTS token_name ON DATABASE TYPE RECORD ALGORITHM HS512 KEY 'secret';"
        self.assertEqual(text, DefineAccessRecord(None, "token_name").algorithm(Algorithm.HS512, "secret").if_not_exists().to_str())
        text = "DEFINE ACCESS OVERWRITE token_name ON DATABASE TYPE RECORD ALGORITHM HS512 KEY 'secret';"
        self.assertEqual(text, DefineAccessRecord(None, "token_name").algorithm(Algorithm.HS512,
                                                                             "secret").overwrite().to_str())


if __name__ == '__main__':
    main()
