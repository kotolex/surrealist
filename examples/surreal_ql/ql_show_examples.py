from datetime import datetime, timezone

from surrealist import Database, to_surreal_datetime_str

# Please read https://docs.surrealdb.com/docs/surrealql/statements/show
# here we represent analogs for string queries

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method
with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db')) as db:
    print(db.table("person").show_changes())  # SHOW CHANGES FOR TABLE person SINCE "2023-09-07T01:23:52Z";

    tm = to_surreal_datetime_str(datetime.now(timezone.utc))  # get current time in surreal format

    # SHOW CHANGES FOR TABLE person SINCE "2023-09-07T01:23:52Z";
    print(db.table("person").show_changes().since(tm))
    print(db.table("person").show_changes(since=tm))

    # SHOW CHANGES FOR TABLE person SINCE "2023-09-07T01:23:52Z" LIMIT 10;
    print(db.person.show_changes().since("2023-09-07T01:23:52Z").limit(10))
    print(db.person.show_changes(since="2023-09-07T01:23:52Z").limit(10))
