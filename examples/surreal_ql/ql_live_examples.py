from surrealist import Database

# Please read https://surrealdb.com/docs/surrealdb/surrealql/statements/live
# here we represent analogs for string queries

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method
with Database("http://127.0.0.1:8000", 'test', 'test', credentials=("user_db", "user_db")) as db:
    # print here - is a callback
    print(db.person.live(print))  # LIVE SELECT * FROM person;
    print(db.person.live(print, use_diff=True))  # LIVE SELECT DIFF FROM person;
    print(db.person.live(print).where("x > 10").fetch("id"))  # LIVE SELECT * FROM person WHERE x > 10 FETCH id;
    print(db.table("person").live(print).value("name"))  # LIVE SELECT VALUE name FROM person;
    print(db.table("person").live(print, select="*, name as author"))  # LIVE SELECT *, name as author FROM person;

    # LIVE SELECT author.id AS auth FROM person WHERE x > 10;
    print(db.person.live(print).alias("author.id", "auth").where("x > 10"))

    # Database object can create LQ too, but you need to specify table name
    print(db.live_query(table_name="person", callback=print, use_diff=True))  # LIVE SELECT DIFF FROM person;
    # LIVE SELECT * FROM person WHERE x > 10 FETCH id;
    print(db.live_query("person", print).where("x > 10").fetch("id"))
