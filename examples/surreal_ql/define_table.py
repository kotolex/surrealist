from surrealist import Database

with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db'), use_http=True) as db:

    # on database object we can DEFINE TABLE
    # https://surrealdb.com/docs/surrealdb/surrealql/statements/define/table
    print(db.define_table("reading"))  # DEFINE TABLE reading;
    print(db.define_table("reading").if_not_exists())  # DEFINE TABLE IF NOT EXISTS reading;
    print(db.define_table("reading").overwrite())  # DEFINE TABLE OVERWRITE reading;
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
    # DEFINE TABLE likes TYPE RELATION FROM user TO post ENFORCED;
    print(db.define_table("likes").type_relation(from_to=("user", "post"), enforced=True))
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