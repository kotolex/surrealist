from surrealist import Database

with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db'), use_http=True) as db:

    # on database object we can DEFINE INDEX
    # https://surrealdb.com/docs/surrealdb/surrealql/statements/define/indexes
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

    # we can rebuild index
    print(db.rebuild_index("userNameIndex", table_name="user"))  # REBUILD INDEX userNameIndex ON TABLE user;
    # REBUILD INDEX IF EXISTS userNameIndex ON TABLE user;
    print(db.rebuild_index("userNameIndex", table_name="user", if_exists=True))

    # we can remove index
    print(db.remove_index("userNameIndex", table_name="user"))  # REMOVE INDEX userNameIndex ON TABLE user;