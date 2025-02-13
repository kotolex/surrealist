from surrealist import Database

with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db')) as db:
    # on database object we can DEFINE FIELD
    # https://surrealdb.com/docs/surrealdb/surrealql/statements/define/field
    print(db.define_field("new_field", "some_table"))  # DEFINE FIELD new_field ON TABLE some_table;
    # DEFINE FIELD IF NOT EXISTS new_field ON TABLE some_table;
    print(db.define_field("new_field", "some_table").if_not_exists())
    # DEFINE FIELD OVERWRITE new_field ON TABLE some_table;
    print(db.define_field("new_field", "some_table").overwrite())
    print(db.define_field("field", "user").type("string"))  # DEFINE FIELD field ON TABLE user TYPE string;
    # DEFINE FIELD field ON TABLE user FLEXIBLE TYPE bool;
    print(db.define_field("field", "user").type("bool", is_flexible=True))
    # DEFINE FIELD locked ON TABLE user TYPE bool DEFAULT false;
    print(db.define_field("locked", "user").type("bool").default("false"))
    # DEFINE FIELD locked ON TABLE user TYPE bool DEFAULT ALWAYS false;
    print(db.define_field("locked", "user").type("bool").default("false", always=True))
    # DEFINE FIELD locked ON TABLE user TYPE bool DEFAULT false COMMENT "Some comment";
    print(db.define_field("locked", "user").type("bool").default("false").comment("Some comment"))
    # DEFINE FIELD updated ON TABLE resource DEFAULT time::now();
    print(db.define_field("updated", "resource").default("time::now()"))
    # DEFINE FIELD updated ON TABLE resource DEFAULT time::now() READONLY;
    print(db.define_field("updated", "resource").default("time::now()").read_only())
    # DEFINE FIELD email ON TABLE resource TYPE string ASSERT string::is::email($value);
    print(db.define_field("email", "resource").type("string").asserts("string::is::email($value)"))
    # DEFINE FIELD comment ON TABLE resource FLEXIBLE TYPE string PERMISSIONS FULL;
    print(db.define_field("comment", "resource").type("string", is_flexible=True).permissions_full())
    # DEFINE FIELD comment ON TABLE resource FLEXIBLE TYPE string PERMISSIONS FULL COMMENT "text";
    print(db.define_field("comment", "resource").type("string", is_flexible=True).permissions_full().comment("text"))
    #  DEFINE FIELD comics ON person TYPE option<array<record<comic_book>>> REFERENCE;
    print(db.define_field("comics", "person"). type("option<array<record<comic_book>>>").reference())
    # DEFINE FIELD comics ON TABLE person TYPE option<array<record<comic_book>>> REFERENCE ON DELETE IGNORE;
    print(db.define_field("comics", "person"). type("option<array<record<comic_book>>>").reference_ignore())
    update = """{
    UPDATE $this SET
        deleted_comments += $reference,
        comments -= $reference;
}"""
#     DEFINE FIELD comics ON TABLE person TYPE option<array<record<comic_book>>> REFERENCE ON DELETE THEN {
#     UPDATE $this SET
#         deleted_comments += $reference,
#         comments -= $reference;
# };
    print(db.define_field("comics", "person").type("option<array<record<comic_book>>>").reference_then(update))