from surrealist import Database

# Please read https://docs.surrealdb.com/docs/surrealql/statements/update
# here we represent analogs for string queries

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method
with Database("http://127.0.0.1:8000", 'test', 'test', ('root', 'root')) as db:
    print(db.table("person").update())  # UPDATE person;
    print(db.table("person").update("tobie").only())  # UPDATE ONLY person:tobie;

    # UPDATE person SET active = true WHERE age < 18 RETURN NONE TIMEOUT 10s PARALLEL;
    print(db.table("person").update().set(active=True).where("age < 18").return_none().timeout("10s").parallel())

    # UPDATE city SET rank = 4, population = 9541000 WHERE name = 'London';
    print(db.city.update().set("rank = 4", population=9541000).where("name = 'London'"))

    # UPDATE city SET rank = 4, population = 9541000 WHERE name = 'London';
    print(db.city.update().set("rank = 4, population = 9541000").where("name = 'London'"))

    # UPDATE city SET rank = 4, population = 9541000 WHERE name = 'London';
    print(db.city.update().set(rank=4, population=9541000).where("name = 'London'"))

    # UPDATE ONLY person:tobie SET name = "Tobie", company = "SurrealDB", skills = ["Rust", "Go", "JavaScript"];
    print(db.table("person").update("tobie").only().set(name="Tobie", company="SurrealDB",
                                                        skills=['Rust', 'Go', 'JavaScript']))

    data = {'name': 'Tobie', 'company': 'SurrealDB', 'skills': ['Rust', 'Go', 'JavaScript']}
    # UPDATE person:tobie CONTENT {"name": "Tobie", "company": "SurrealDB", "skills": ["Rust", "Go", "JavaScript"]};
    print(db.person.update("tobie").content(data))

    data = [{"op": "add", "path": "Engineering", "value": "true"}]
    # UPDATE person:tobie PATCH [{"op": "add", "path": "Engineering", "value": "true"}];
    print(db.person.update("tobie").patch(data))

    data = {'settings': {'marketing': True}}
    # UPDATE person:tobie MERGE {"settings": {"marketing": true}};
    print(db.person.update("tobie").merge(data))
