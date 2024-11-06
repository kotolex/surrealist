from surrealist import Database

# Please read https://docs.surrealdb.com/docs/surrealql/statements/relate
# here we represent analogs for string queries

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method
with Database("http://127.0.0.1:8000", 'test', 'test', credentials=("user_db", "user_db")) as db:
    # Pay attention, we should use RELATE on database level, not on specific table

    # RELATE person:l19zjikkw1p1h9o6ixrg->wrote->article:8nkk6uj4yprt49z7y3zm;
    print(db.relate("person:l19zjikkw1p1h9o6ixrg->wrote->article:8nkk6uj4yprt49z7y3zm"))

    # RELATE person:l19zjikkw1p1h9o6ixrg->wrote->article:8nkk6uj4yprt49 SET time.written = time::now();
    print(db.relate("person:l19zjikkw1p1h9o6ixrg->wrote->article:8nkk6uj4yprt49").set("time.written = time::now()"))

    # RELATE person:john->wrote->article:best SET title = "title", text = "text";
    print(db.relate("person:john->wrote->article:best").set('title = "title"', text="text"))

    # RELATE person:john->wrote->article:best SET title = "title", text = "text";
    print(db.relate("person:john->wrote->article:best").set(title="title", text="text"))

    # RELATE person:john->wrote->article:best SET title = "title", text = "text";
    print(db.relate("person:john->wrote->article:best").set('title = "title", text = "text"'))

    # RELATE person:l19zjikkw1p1h9o6ixrg->wrote->article:8nkk6uj4yprt49z7y3zm
    #     CONTENT {
    #         time: {
    #             written: time::now()
    #         }
    #     };
    data = {"time": {"written": "time::now()"}}
    print(db.relate("person:l19zjikkw1p1h9o6ixrg->wrote->article:8nkk6uj4yprt49z7y3zm").content(data))

    # RELATE person:john->wrote->article:main SET time.written = time::now() RETURN NONE;
    print(db.relate("person:john->wrote->article:main").set("time.written = time::now()").return_none())
    # RELATE person:john->wrote->article:main SET time.written = time::now() RETURN AFTER;
    print(db.relate("person:john->wrote->article:main").set("time.written = time::now()").return_after())
    # RELATE person:john->wrote->article:main SET time.written = time::now() RETURN BEFORE;
    print(db.relate("person:john->wrote->article:main").set("time.written = time::now()").return_before())
    # RELATE person:john->wrote->article:main SET time.written = time::now() RETURN DIFF;
    print(db.relate("person:john->wrote->article:main").set("time.written = time::now()").return_diff())
    # RELATE person:john->wrote->article:main SET time.written = time::now() RETURN time;
    print(db.relate("person:john->wrote->article:main").set("time.written = time::now()").returns("time"))
