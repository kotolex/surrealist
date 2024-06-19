from surrealist import Database

# Please read https://docs.surrealdb.com/docs/surrealql/statements/upsert
# here we represent analogs for string queries

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method
with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db')) as db:
    print(db.table("person").upsert())  # UPSERT person;
    print(db.table("person").upsert("tobie").only())  # UPSERT ONLY person:tobie;

    # UPSERT person SET active = true WHERE age < 18 RETURN NONE TIMEOUT 10s PARALLEL;
    print(db.table("person").upsert().set(active=True).where("age < 18").return_none().timeout("10s").parallel())

    # UPSERT city SET rank = 4, population = 9541000 WHERE name = 'London';
    print(db.city.upsert().set("rank = 4", population=9541000).where("name = 'London'"))

    # UPSERT city SET rank = 4, population = 9541000 WHERE name = 'London';
    print(db.city.upsert().set("rank = 4, population = 9541000").where("name = 'London'"))

    # UPSERT city SET rank = 4, population = 9541000 WHERE name = 'London';
    print(db.city.upsert().set(rank=4, population=9541000).where("name = 'London'"))

    # UPSERT ONLY person:tobie SET name = "Tobie", company = "SurrealDB", skills = ["Rust", "Go", "JavaScript"];
    print(db.table("person").upsert("tobie").only().set(name="Tobie", company="SurrealDB",
                                                        skills=['Rust', 'Go', 'JavaScript']))

    data = {'name': 'Tobie', 'company': 'SurrealDB', 'skills': ['Rust', 'Go', 'JavaScript']}
    # UPSERT person:tobie CONTENT {"name": "Tobie", "company": "SurrealDB", "skills": ["Rust", "Go", "JavaScript"]};
    print(db.person.upsert("tobie").content(data))

    data = [{"op": "add", "path": "Engineering", "value": "true"}]
    # UPSERT person:tobie PATCH [{"op": "add", "path": "Engineering", "value": "true"}];
    print(db.person.upsert("tobie").patch(data))

    data = {'settings': {'marketing': True}}
    # UPSERT person:tobie MERGE {"settings": {"marketing": true}};
    print(db.person.upsert("tobie").merge(data))
