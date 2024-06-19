from surrealist import Database

# Please read https://docs.surrealdb.com/docs/surrealql/statements/delete
# here we represent analogs for string queries

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method
with Database("http://127.0.0.1:8000", 'test', 'test', ('root', 'root'), use_http=True) as db:
    print(db.person.delete())  # DELETE person;
    print(db.person.delete("tobie"))  # DELETE person:tobie;
    print(db.table("person").delete("tobie").only())  # DELETE ONLY person:tobie;

    # DELETE person WHERE age < 18 RETURN NONE TIMEOUT 10s PARALLEL;
    print(db.table("person").delete().where("age < 18").return_none().timeout("10s").parallel())

    # DELETE person WHERE age < 18 RETURN AFTER;
    print(db.table("person").delete().where("age < 18").return_after())

    # DELETE person WHERE age < 18 RETURN BEFORE;
    print(db.table("person").delete().where("age < 18").return_before())

    # DELETE person WHERE age < 18 RETURN NONE;
    print(db.table("person").delete().where("age < 18").return_none())

    # DELETE person WHERE age < 18 RETURN id;
    print(db.table("person").delete().where("age < 18").returns("id"))
