from surrealist import Database

# Note: you can use only database level connection for Database object! So, if you want to use root or namespace user,
# you should use Connection object and call USE first.
# See example here: https://github.com/kotolex/surrealist/tree/master/examples/connect.py

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method

with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db')) as db:
    # ACCESS api ON DATABASE GRANT FOR USER automation;
    print(db.access("api").grant_for_user("automation"))
    # ACCESS api ON DATABASE GRANT FOR RECORD record:1;
    print(db.access("api").grant_for_record("record:1"))
    # ACCESS api ON DATABASE SHOW ALL;
    print(db.access("api").show_all())
    # ACCESS api ON DATABASE SHOW GRANT some_id;
    print(db.access("api").show_grant("some_id"))
    # ACCESS api ON DATABASE SHOW WHERE subject.record.name = 'tobie';
    print(db.access("api").show_where("subject.record.name = 'tobie'"))
    # ACCESS api ON DATABASE REVOKE ALL;
    print(db.access("api").revoke_all())
    # ACCESS api ON DATABASE REVOKE GRANT some_id;
    print(db.access("api").revoke_grant("some_id"))
    # ACCESS api ON DATABASE REVOKE WHERE subject.record.name = 'tobie';
    print(db.access("api").revoke_where("subject.record.name = 'tobie'"))
    # ACCESS api ON DATABASE PURGE EXPIRED;
    print(db.access("api").purge_expired())
    # ACCESS api ON DATABASE PURGE REVOKED;
    print(db.access("api").purge_revoked())
    # ACCESS api ON DATABASE PURGE EXPIRED, PURGE REVOKED 1y;
    print(db.access("api").purge_expired().purge_revoked("1y"))

