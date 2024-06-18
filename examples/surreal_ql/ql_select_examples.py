from surrealist import Database

# Please read https://docs.surrealdb.com/docs/surrealql/statements/select
# here we represent analogs for string queries

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method
with Database("http://127.0.0.1:8000", 'test', 'test', ('user_db', 'user_db')) as db:
    print(db.table("person").select())  # SELECT * FROM person;
    print(db.table("person").select("*"))  # SELECT * FROM person;
    print(db.table("person").select().by_id("john"))  # SELECT * FROM person:john;
    print(db.table("person").select("name", "age"))  # SELECT name, age FROM person;
    print(db.table("person").select("name", "age").by_id("john").only())  # SELECT name, age FROM ONLY person:john;
    print(db.table("person").select(value="age").by_id("john"))  # SELECT VALUE age FROM person:john;

    # SELECT *, (SELECT * FROM events WHERE type = 'activity' LIMIT 5) AS history FROM person;
    print(db.table("person").select("*", alias=[("history", "(SELECT * FROM events WHERE type = 'activity' LIMIT 5)")]))

    # here we're using another statement(select) to select
    select = db.events.select().where("type = 'activity'").limit(5)
    # SELECT *, (SELECT * FROM events WHERE type = 'activity' LIMIT 5) AS history FROM person;
    print(db.table("person").select("*", alias=[("history", select)]))

    # SELECT array::group(tags) AS tags FROM article GROUP ALL;
    print(db.article.select(alias=[("tags", "array::group(tags)")]).group_all())

    print(db.person.select().omit("password", "opts.security"))  # SELECT * OMIT password, opts.security FROM person;
    print(db.user.select().split("emails"))  # SELECT * FROM user SPLIT emails;
    print(db.user.select("country").group_by("country"))  # SELECT country FROM user GROUP BY country;

    # SELECT count() AS number_of_records FROM person GROUP ALL;
    print(db.person.select(alias=[("number_of_records", "count()")]).group_all())

    print(db.person.select().order_by_rand())  # SELECT * FROM person ORDER BY RAND();

    # SELECT * FROM song ORDER BY artist ASC, rating DESC;
    print(db.song.select().order_by("artist ASC", "rating DESC"))
    print(db.account.select().limit(50).start_at(50))  # SELECT * FROM account LIMIT 50 START 50;

    # When processing a large result set with many records, it is possible to use the TEMPFILES clause to specify that
    # the statement should be processed in temporary files rather than memory. This significantly reduces memory usage,
    # though it will also result in slower performance.
    # SELECT * FROM person WHERE email='tobie@surrealdb.com' TEMPFILES EXPLAIN;
    print(db.table("person").select().where("email='tobie@surrealdb.com'").tempfiles().explain())

    # SELECT * FROM person WHERE email='tobie@surrealdb.com' EXPLAIN;
    print(db.person.select().where("email='tobie@surrealdb.com'").explain())

    # SELECT * FROM person WHERE email='tobie@surrealdb.com' EXPLAIN FULL;
    print(db.person.select().where("email='tobie@surrealdb.com'").explain_full())

    # SELECT name FROM person WITH INDEX idx_name WHERE job='engineer' AND genre = 'm';
    print(db.person.select("name").with_index("idx_name").where("job='engineer' AND genre = 'm'"))

    # AND / OR helpers to concatenate with WHERE
    # SELECT name FROM person WITH INDEX idx_name WHERE job='engineer' AND genre = 'm';
    print(db.person.select("name").with_index("idx_name").where("job='engineer'").AND("genre = 'm'"))

    # If you need to select from other select (sub-query), you need to use database, not table
    # SELECT * FROM (SELECT * FROM events WHERE type = 'active') LIMIT 5 PARALLEL;
    sub_query = db.events.select().where("type = 'active'")
    print(db.select_from(sub_query).limit(5).parallel())
