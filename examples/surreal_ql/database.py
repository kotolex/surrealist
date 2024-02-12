from surrealist import Database

with Database("http://127.0.0.1:8000", 'test', 'test', ('root', 'root')) as db:
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

    # DEFINE examples below you can see https://docs.surrealdb.com/docs/surrealql/statements/define/overview

    # on database object we can use DEFINE EVENT with sub-query
    # DEFINE EVENT email ON TABLE user WHEN $before.email != $after.email THEN (CREATE event SET user = $value.id,
    # time = time::now(), value = $after.email);
    then = db.event.create().set("user = $value.id, time = time::now(), value = $after.email")
    print(db.define_event("email", table_name="user", then=then).when("$before.email != $after.email"))

    # we can REMOVE EVENT
    print(db.remove_event("email", table_name="user"))  # REMOVE EVENT email ON TABLE user;

    # on database object we can DEFINE USER
    # DEFINE USER new_user ON DATABASE PASSWORD '123456' ROLES OWNER;
    print(db.define_user("new_user", "123456").role_owner())
    # we can remove user
    print(db.remove_user("new_user"))  # REMOVE USER new_user ON DATABASE;

    # on database object we can DEFINE PARAM
    # DEFINE PARAM $key VALUE 1000;
    print(db.define_param("key", 1000))
    # we can remove parameter
    print(db.remove_param("key"))  # REMOVE PARAM $key;

    # on database object we can DEFINE ANALYZER
    # DEFINE ANALYZER example_ascii TOKENIZERS class FILTERS ascii;
    print(db.define_analyzer("example_ascii").tokenizers("class").filters("ascii"))
    # we can remove analyzer
    print(db.remove_analyzer("example_ascii"))  # REMOVE ANALYZER example_ascii;

    # on database object we can DEFINE SCOPE
    # but first, let's generate Create and Select queries for our scope users
    create = db.user.create().set("email = $email, pass = crypto::argon2::generate($pass)")
    select = db.user.select().where("email = $email AND crypto::argon2::compare(pass, $pass)")

    # DEFINE SCOPE account SESSION 24h
    # SIGNUP (CREATE user SET email = $email, pass = crypto::argon2::generate($pass))
    # SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass));
    print(db.define_scope("account", "24h", signup=create, signin=select))
    # we can remove scope
    print(db.remove_scope("account"))  # REMOVE SCOPE account;

    # on database object we can DEFINE INDEX
    # DEFINE INDEX userEmailIndex ON TABLE user COLUMNS email UNIQUE;
    print(db.define_index("userEmailIndex", "user").columns("email").unique())
    # DEFINE INDEX userAgeIndex ON TABLE user COLUMNS age;
    print(db.define_index("userAgeIndex", "user").columns("age"))
    # DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii BM25 HIGHLIGHTS;
    print(db.define_index("userNameIndex", "user").columns("name").search_analyzer("ascii"))

    # we can remove index
    print(db.remove_index("userNameIndex", table_name="user"))  # REMOVE INDEX userNameIndex ON TABLE user;

    # on database object we can DEFINE TOKEN
    # DEFINE TOKEN token_name ON DATABASE
    # TYPE HS512
    # VALUE "sNSYneezcr8kqphfOC6NwwraUHJCVAt0XjsRSNmssBaBRh3WyMa9TRfq8ST7fsU2H2kGiOpU4GbAF1bCiXmM1b3JGgleBzz7rsrz";
    token_value = "sNSYneezcr8kqphfOC6NwwraUHJCVAt0XjsRSNmssBaBRh3WyMa9TRfq8ST7fsU2H2kGiOpU4GbAF1bCiXmM1b3JGgleBzz7rsrz"
    print(db.define_token("token_name", "HS512", value=token_value))

    # we can remove token by name
    print(db.remove_token("token_name"))  # REMOVE TOKEN token_name ON DATABASE;
