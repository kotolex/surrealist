from surrealist import AutoOrNone, Database

with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db'), use_http=True) as db:
    # DEFINE CONFIG GRAPHQL AUTO;
    print(db.define_config(AutoOrNone.AUTO))

    #DEFINE CONFIG GRAPHQL TABLES AUTO;
    print(db.define_config().tables_kind(AutoOrNone.AUTO))

    # DEFINE CONFIG GRAPHQL TABLES INCLUDE user, post, comment;
    print(db.define_config().tables_include("user, post, comment"))

    # DEFINE CONFIG GRAPHQL FUNCTIONS NONE;
    print(db.define_config().functions_kind(AutoOrNone.NONE))

    # DEFINE CONFIG GRAPHQL FUNCTIONS INCLUDE [getUser, listPosts, searchComments];
    print(db.define_config().functions_include("[getUser, listPosts, searchComments]"))

    # DEFINE CONFIG IF NOT EXISTS GRAPHQL TABLES AUTO FUNCTIONS AUTO;
    print(db.define_config().if_not_exists().tables_kind(AutoOrNone.AUTO).functions_kind(AutoOrNone.AUTO))

    # DEFINE CONFIG OVERWRITE GRAPHQL TABLES AUTO FUNCTIONS AUTO;
    print(db.define_config().overwrite().tables_kind(AutoOrNone.AUTO).functions_kind(AutoOrNone.AUTO))


