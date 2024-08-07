from surrealist import Database

# Note: you can use only database level connection for Database object! So, if you want to use root or namespace user,
# you should use Connection object and call USE first.
# See example here: https://github.com/kotolex/surrealist/tree/master/examples/connect.py

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method

with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db')) as db:
    # ALTER TABLE user SCHEMAFULL;
    print(db.alter_table("user").schema_full())
    # ALTER TABLE IF NOT EXISTS user SCHEMALESS;
    print(db.alter_table("user").if_not_exists().schema_less())
    # ALTER TABLE user PERMISSIONS FOR create FULL;
    print(db.alter_table("user").permissions_for(create="FULL"))
    # ALTER TABLE user PERMISSIONS NONE COMMENT "text_of_comment";
    print(db.alter_table("user").permissions_none().comment("text_of_comment"))
    # ALTER TABLE IF NOT EXISTS user SCHEMAFULL;
    print(db.alter_table("user").if_not_exists().schema_full())





