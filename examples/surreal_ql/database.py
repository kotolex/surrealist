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

    # database can run built-in functions
    print(db.run_function("math::abs", None, [-100]).result)  # returns 100 (math::abs(-100))

    # database can use RETURN statement
    # Refer to: https://docs.surrealdb.com/docs/surrealql/statements/return
    print(db.returns("math::abs(-100)"))  # RETURN math::abs(-100);

    # on database object we can use DEFINE EVENT with sub-query
    # https://surrealdb.com/docs/surrealql/statements/define/event
    # DEFINE EVENT email ON TABLE user WHEN $before.email != $after.email THEN (CREATE event SET user = $value.id,
    # time = time::now(), value = $after.email);
    then = db.event.create().set("user = $value.id, time = time::now(), value = $after.email")
    print(db.define_event("email", table_name="user", then=then).when("$before.email != $after.email"))

    # we can REMOVE EVENT
    print(db.remove_event("email", table_name="user"))  # REMOVE EVENT email ON TABLE user;

    # on database object we can DEFINE PARAM
    # https://surrealdb.com/docs/surrealql/statements/define/param
    print(db.define_param("key", 1000))  # DEFINE PARAM $key VALUE 1000;
    print(db.define_param("key", 1000).if_not_exists())  # DEFINE PARAM IF NOT EXISTS $key VALUE 1000;
    print(db.define_param("key", 1000).overwrite())  # DEFINE PARAM OVERWRITE $key VALUE 1000;
    # we can remove parameter
    print(db.remove_param("key"))  # REMOVE PARAM $key;

    # on database object we can DEFINE ANALYZER
    # https://surrealdb.com/docs/surrealql/statements/define/analyzer
    # DEFINE ANALYZER example_ascii TOKENIZERS class FILTERS ascii;
    print(db.define_analyzer("example_ascii").tokenizer_class().filter_ascii())
    # DEFINE ANALYZER IF NOT EXISTS example_ascii TOKENIZERS class FILTERS ascii;
    print(db.define_analyzer("example_ascii").if_not_exists().tokenizer_class().filter_ascii())
    # DEFINE ANALYZER OVERWRITE example_ascii TOKENIZERS class FILTERS ascii;
    print(db.define_analyzer("example_ascii").overwrite().tokenizer_class().filter_ascii())
    # DEFINE ANALYZER example_ascii TOKENIZERS class, camel FILTERS ascii, lowercase;
    print(db.define_analyzer("example_ascii").tokenizer_class().tokenizer_camel().filter_lowercase().filter_ascii())
    # DEFINE ANALYZER example_ascii FILTERS lowercase, snowball(english);
    print(db.define_analyzer("example_ascii").filter_lowercase().filter_snowball("english"))
    # we can remove analyzer
    print(db.remove_analyzer("example_ascii"))  # REMOVE ANALYZER example_ascii;
