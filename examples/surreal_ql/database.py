from surrealist import Database

with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db'), use_http=True) as db:
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

    # DEFINE examples below you can see https://surrealdb.com/docs/surrealdb/surrealql/statements/define/

    # on database object we can DEFINE TABLE
    print(db.define_table("reading"))  # DEFINE TABLE reading;
    print(db.define_table("reading").if_not_exists())  # DEFINE TABLE IF NOT EXISTS reading;
    print(db.define_table("reading").drop())  # DEFINE TABLE reading DROP;
    print(db.define_table("reading").changefeed("1h"))  # DEFINE TABLE reading CHANGEFEED 1h;
    # DEFINE TABLE reading CHANGEFEED 1h COMMENT "Some comment";
    print(db.define_table("reading").changefeed("1h").comment("Some comment"))

    # DEFINE TABLE reading CHANGEFEED 1h INCLUDE ORIGINAL;
    print(db.define_table("reading").changefeed("1h", include_original=True))
    print(db.define_table("user").schemafull())  # DEFINE TABLE user SCHEMAFULL;
    print(db.define_table("user").schemaless())  # DEFINE TABLE user SCHEMALESS;
    print(db.define_table("person").type_any())  # DEFINE TABLE person TYPE ANY;
    print(db.define_table("person").type_normal())  # DEFINE TABLE person TYPE NORMAL;
    print(db.define_table("likes").type_relation())  # DEFINE TABLE likes TYPE RELATION;

    # DEFINE TABLE likes TYPE RELATION FROM user TO post;
    print(db.define_table("likes").type_relation(from_to=("user", "post")))
    # DEFINE TABLE likes TYPE RELATION IN user OUT post;
    print(db.define_table("likes").type_relation(from_to=("user", "post"), use_from_to=False))

    # DEFINE TABLE temperatures_by_month AS
    #  SELECT count() AS total, time::month(recorded_at) AS month FROM reading GROUP BY city
    # ;
    select = db.table("reading").select(alias=[("total", "count()"), ("month", "time::month(recorded_at)")]). \
        group_by("city")
    print(db.define_table("temperatures_by_month").alias(select))

    print(db.define_table("account").permissions_none())  # DEFINE TABLE account PERMISSIONS NONE;
    print(db.define_table("account").permissions_full())  # DEFINE TABLE account PERMISSIONS FULL;

    # we can use simple Where to generate statements with permissions
    from surrealist import Where

    select = Where(published=True).OR('user = "$auth.id"')
    create = Where('user = "$auth.id"')
    delete = Where('user = "$auth.id"').OR("$auth.admin = true")
    # DEFINE TABLE post SCHEMALESS
    # PERMISSIONS
    #  FOR select WHERE published = true OR user = $auth.id
    #  FOR create, update WHERE user = $auth.id
    #  FOR delete WHERE user = $auth.id OR $auth.admin = true;
    print(db.define_table("post").schemaless().permissions_for(select=select, create=create, update=create,
                                                               delete=delete))

    # on database object we can DEFINE FIELD
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
    # DEFINE EVENT email ON TABLE user WHEN $before.email != $after.email THEN (CREATE event SET user = $value.id,
    # time = time::now(), value = $after.email);
    then = db.event.create().set("user = $value.id, time = time::now(), value = $after.email")
    print(db.define_event("email", table_name="user", then=then).when("$before.email != $after.email"))

    # we can REMOVE EVENT
    print(db.remove_event("email", table_name="user"))  # REMOVE EVENT email ON TABLE user;

    # on database object we can DEFINE USER
    # DEFINE USER new_user ON DATABASE PASSWORD '123456' ROLES OWNER;
    print(db.define_user("new_user", "123456").role_owner())
    # DEFINE USER IF NOT EXISTS new_user ON DATABASE PASSWORD '123456' ROLES OWNER;
    print(db.define_user("new_user", "123456").if_not_exists().role_owner())
    # we can remove user
    print(db.remove_user("new_user"))  # REMOVE USER new_user ON DATABASE;

    # on database object we can DEFINE PARAM
    print(db.define_param("key", 1000))  # DEFINE PARAM $key VALUE 1000;
    print(db.define_param("key", 1000).if_not_exists())  # DEFINE PARAM IF NOT EXISTS $key VALUE 1000;
    # we can remove parameter
    print(db.remove_param("key"))  # REMOVE PARAM $key;

    # on database object we can DEFINE ANALYZER
    # DEFINE ANALYZER example_ascii TOKENIZERS class FILTERS ascii;
    print(db.define_analyzer("example_ascii").tokenizers("class").filters("ascii"))
    # DEFINE ANALYZER IF NOT EXISTS example_ascii TOKENIZERS class FILTERS ascii;
    print(db.define_analyzer("example_ascii").if_not_exists().tokenizers("class").filters("ascii"))
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
    # DEFINE INDEX userEmailIndex ON TABLE user COLUMNS email UNIQUE COMMENT "unique index";
    print(db.define_index("userEmailIndex", "user").columns("email").unique().comment("unique index"))
    # DEFINE INDEX IF NOT EXISTS userEmailIndex ON TABLE user COLUMNS email UNIQUE;
    print(db.define_index("userEmailIndex", "user").if_not_exists().columns("email").unique())
    # DEFINE INDEX userAgeIndex ON TABLE user COLUMNS age;
    print(db.define_index("userAgeIndex", "user").columns("age"))
    # DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii BM25 HIGHLIGHTS;
    print(db.define_index("userNameIndex", "user").columns("name").search_analyzer("ascii").bm25().highlights())
    # DEFINE INDEX userNameIndex ON TABLE user COLUMNS name SEARCH ANALYZER ascii BM25 1.2 0.75;
    print(db.define_index("userNameIndex", "user").columns("name").search_analyzer("ascii").bm25(1.2, 0.75))

    # DEFINE INDEX mtree ON TABLE user FIELDS name MTREE DIMENSION 4 DIST EUCLIDEAN;
    print(db.define_index("mtree", "user").fields("name").mtree(4).distance_euclidean())
    # DEFINE INDEX hnsw ON TABLE user FIELDS name HNSW DIMENSION 4 DIST COSINE EFC 150 M 2;
    print(db.define_index("hnsw", "user").fields("name").hnsw(4).distance_cosine().efc(150).max_connections(2))
    # DEFINE INDEX IF NOT EXISTS mtree ON TABLE user FIELDS name MTREE DIMENSION 3 DIST MANHATTAN;
    print(db.define_index("mtree", "user").if_not_exists().fields("name").mtree(3).distance_manhattan())
    # DEFINE INDEX idx_mtree_embedding ON TABLE Document FIELDS items.embedding MTREE DIMENSION 4 TYPE I64;
    print(db.define_index("idx_mtree_embedding", "Document").fields("items.embedding").mtree(4).type_i64())

    # we can remove index
    print(db.remove_index("userNameIndex", table_name="user"))  # REMOVE INDEX userNameIndex ON TABLE user;

    # we can rebuild index
    print(db.rebuild_index("userNameIndex", table_name="user"))  # REBUILD INDEX userNameIndex ON TABLE user;
    # REBUILD INDEX IF EXISTS userNameIndex ON TABLE user;
    print(db.rebuild_index("userNameIndex", table_name="user", if_exists=True))

    # on database object we can DEFINE TOKEN
    # DEFINE TOKEN token_name ON DATABASE
    # TYPE HS512
    # VALUE "sNSYneezcr8kqphfOC6NwwraUHJCVAt0XjsRSNmssBaBRh3WyMa9TRfq8ST7fsU2H2kGiOpU4GbAF1bCiXmM1b3JGgleBzz7rsrz";
    token_value = "sNSYneezcr8kqphfOC6NwwraUHJCVAt0XjsRSNmssBaBRh3WyMa9TRfq8ST7fsU2H2kGiOpU4GbAF1bCiXmM1b3JGgleBzz7rsrz"
    print(db.define_token("token_name", "HS512", value=token_value))

    # we can remove token by name
    print(db.remove_token("token_name"))  # REMOVE TOKEN token_name ON DATABASE;
