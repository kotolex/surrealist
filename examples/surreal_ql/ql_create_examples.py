from surrealist import Database

# Please read https://docs.surrealdb.com/docs/surrealql/statements/create
# here we represent analogs for string queries

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method
with Database("http://127.0.0.1:8000", 'test', 'test', credentials=("user_db", "user_db")) as db:
    print(db.person.create())  # CREATE person;
    print(db.table("person").create().only())  # CREATE ONLY person;
    print(db.table("person").create("tobie").only())  # CREATE ONLY person:tobie;

    # CREATE person SET name = "Tobie", company = "SurrealDB", skills = ["Rust", "Go", "JavaScript"];
    print(db.table("person").create().set(name="Tobie", company="SurrealDB", skills=["Rust", "Go", "JavaScript"]))

    # CREATE person SET name="Tobie", company="SurrealDB", skills=["Rust", "Go", "JavaScript"];
    print(db.table("person").create().set('name="Tobie", company="SurrealDB", skills=["Rust", "Go", "JavaScript"]'))

    # CREATE person SET name = "Tobie", company = "SurrealDB", skills = ["Rust", "Go", "JavaScript"];
    print(db.table("person").create().set('name = "Tobie"', company="SurrealDB", skills=["Rust", "Go", "JavaScript"]))

    # CREATE person:tobie CONTENT {"name": "Tobie", "company": "SurrealDB", "skills": ["Rust"]};
    print(db.table("person").create("tobie").content({"name": "Tobie", "company": "SurrealDB", "skills": ["Rust"]}))

    # CREATE person SET age = 46, username = "john-smith" RETURN NONE;
    print(db.person.create().set(age=46, username="john-smith").return_none())

    # CREATE person SET age = 46, username = "john-smith" RETURN AFTER;
    print(db.person.create().set(age=46, username="john-smith").return_after())

    # CREATE person SET age = 46, username = "john-smith" RETURN BEFORE;
    print(db.person.create().set(age=46, username="john-smith").return_before())

    # CREATE person SET age = 46, username = "john-smith" RETURN interests;
    print(db.person.create().set(age=46, username="john-smith").returns("interests"))





