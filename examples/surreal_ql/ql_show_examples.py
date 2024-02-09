from surrealist import Database

# Please read https://docs.surrealdb.com/docs/surrealql/statements/show
# here we represent analogs for string queries

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method
with Database("http://127.0.0.1:8000", 'test', 'test', ('root', 'root')) as db:
    print(db.table("person").show_changes()) # SHOW CHANGES FOR TABLE person;

    # SHOW CHANGES FOR TABLE person SINCE "2023-09-07T01:23:52Z";
    print(db.table("person").show_changes().since("2023-09-07T01:23:52Z"))

    # SHOW CHANGES FOR TABLE person SINCE "2023-09-07T01:23:52Z" LIMIT 10;
    print(db.person.show_changes().since("2023-09-07T01:23:52Z").limit(10))