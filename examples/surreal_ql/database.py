from surrealist import Database

# Note: you can use only database level connection for Database object! So, if you want to use root or namespace user,
# you should use Connection object and call USE first.
# See example here: https://github.com/kotolex/surrealist/tree/master/examples/connect.py


with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db')) as db:
    print(db)  # Database(namespace=test, name=test, connected=True)
    # The database is empty, so on next request we get
    # {'analyzers': {}, 'functions': {}, 'models': {}, 'params': {}, 'scopes': {}, 'tables': {}, 'tokens': {},
    # 'users': {}}
    print(db.info())
    print(db.tables())  # []
    # below, you see two methods of using table: via dot and via method
    print(db.person)  # Table(name=person)
    print(db.table("person"))  # Table(name=person)

    # database can use RETURN statement
    # Refer to: https://docs.surrealdb.com/docs/surrealql/statements/return
    print(db.returns("math::abs(-100)"))  # RETURN math::abs(-100);

    # on database object we can DEFINE FIELD
    # https://surrealdb.com/docs/surrealdb/surrealql/statements/define/field
    print(db.define_field("new_field", "some_table"))  # DEFINE FIELD new_field ON TABLE some_table;
    # DEFINE FIELD IF NOT EXISTS new_field ON TABLE some_table;
    print(db.define_field("new_field", "some_table").if_not_exists())
    print(db.define_field("field", "user").type("string"))  # DEFINE FIELD field ON TABLE user TYPE string;
    # DEFINE FIELD field ON TABLE user FLEXIBLE TYPE bool;
    print(db.define_field("field", "user").type("bool", is_flexible=True))
    # DEFINE FIELD locked ON TABLE user TYPE bool DEFAULT false;
    print(db.define_field("locked", "user").type("bool").default("false"))
    # DEFINE FIELD locked ON TABLE user TYPE bool DEFAULT false COMMENT "Some comment";
    print(db.define_field("locked", "user").type("bool").default("false").comment("Some comment"))
    # DEFINE FIELD updated ON TABLE resource DEFAULT time::now();
    print(db.define_field("updated", "resource").default("time::now()"))
    # DEFINE FIELD updated ON TABLE resource DEFAULT time::now() READONLY;
    print(db.define_field("updated", "resource").default("time::now()").read_only())
    # DEFINE FIELD email ON TABLE resource TYPE string ASSERT string::is::email($value);
    print(db.define_field("email", "resource").type("string").asserts("string::is::email($value)"))
    # DEFINE FIELD comment ON TABLE resource FLEXIBLE TYPE string PERMISSIONS FULL;
    print(db.define_field("comment", "resource").type("string", is_flexible=True).permissions_full())

    # on database object we can use DEFINE EVENT with sub-query
    # https://surrealdb.com/docs/surrealdb/surrealql/statements/define/event
    # DEFINE EVENT email ON TABLE user WHEN $before.email != $after.email THEN (CREATE event SET user = $value.id,
    # time = time::now(), value = $after.email);
    then = db.event.create().set("user = $value.id, time = time::now(), value = $after.email")
    print(db.define_event("email", table_name="user", then=then).when("$before.email != $after.email"))

    # we can REMOVE EVENT
    print(db.remove_event("email", table_name="user"))  # REMOVE EVENT email ON TABLE user;

    # on database object we can DEFINE PARAM
    # https://surrealdb.com/docs/surrealdb/surrealql/statements/define/param
    print(db.define_param("key", 1000))  # DEFINE PARAM $key VALUE 1000;
    print(db.define_param("key", 1000).if_not_exists())  # DEFINE PARAM IF NOT EXISTS $key VALUE 1000;
    # we can remove parameter
    print(db.remove_param("key"))  # REMOVE PARAM $key;

    # on database object we can DEFINE ANALYZER
    # https://surrealdb.com/docs/surrealdb/surrealql/statements/define/analyzer
    # DEFINE ANALYZER example_ascii TOKENIZERS class FILTERS ascii;
    print(db.define_analyzer("example_ascii").tokenizer_class().filter_ascii())
    # DEFINE ANALYZER IF NOT EXISTS example_ascii TOKENIZERS class FILTERS ascii;
    print(db.define_analyzer("example_ascii").if_not_exists().tokenizer_class().filter_ascii())
    # DEFINE ANALYZER example_ascii TOKENIZERS class, camel FILTERS ascii, lowercase;
    print(db.define_analyzer("example_ascii").tokenizer_class().tokenizer_camel().filter_lowercase().filter_ascii())
    # DEFINE ANALYZER example_ascii FILTERS lowercase, snowball(english);
    print(db.define_analyzer("example_ascii").filter_lowercase().filter_snowball("english"))
    # we can remove analyzer
    print(db.remove_analyzer("example_ascii"))  # REMOVE ANALYZER example_ascii;

    # on database object we can DEFINE SCOPE
    # DEFINE SCOPE is deprecated since SurrealDB 2.x, use DEFINE ACCESS instead
    # but first, let's generate Create and Select queries for our scope users
    create = db.user.create().set("email = $email, pass = crypto::argon2::generate($pass)")
    select = db.user.select().where("email = $email AND crypto::argon2::compare(pass, $pass)")

    # DEFINE SCOPE account SESSION 24h
    # SIGNUP (CREATE user SET email = $email, pass = crypto::argon2::generate($pass))
    # SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass));
    print(db.define_scope("account", "24h", signup=create, signin=select))
    # we can remove scope
    print(db.remove_scope("account"))  # REMOVE SCOPE account;

    # on database object we can DEFINE TOKEN
    # DEFINE TOKEN is deprecated since SurrealDB 2.x, use DEFINE ACCESS instead
    from surrealist import Algorithm  # need to specify algorithm for token

    # DEFINE TOKEN token_name ON DATABASE
    # TYPE HS512
    # VALUE "sNSYneezcr8kqphfOC6NwwraUHJCVAt0XjsRSNmssBaBRh3WyMa9TRfq8ST7fsU2H2kGiOpU4GbAF1bCiXmM1b3JGgleBzz7rsrz";
    token_value = "sNSYneezcr8kqphfOC6NwwraUHJCVAt0XjsRSNmssBaBRh3WyMa9TRfq8ST7fsU2H2kGiOpU4GbAF1bCiXmM1b3JGgleBzz7rsrz"
    print(db.define_token("token_name", Algorithm.HS512, value=token_value))

    # we can remove token by name
    print(db.remove_token("token_name"))  # REMOVE TOKEN token_name ON DATABASE;
