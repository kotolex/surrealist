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
