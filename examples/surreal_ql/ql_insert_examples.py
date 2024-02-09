from surrealist import Database

# Please read https://docs.surrealdb.com/docs/surrealql/statements/insert
# here we represent analogs for string queries

# Notice: all queries below not executed, just generate representation.
# To run it against SurrealDB, you need to use run method
with Database("http://127.0.0.1:8000", 'test', 'test', ('root', 'root'), use_http=True) as db:
    data = {
        'name': 'SurrealDB',
        'founded': "2021-09-10",
        'founders': ['person:tobie', 'person:jaime'],
        'tags': ['big data', 'database']
    }
    # INSERT INTO person {"name": "SurrealDB", "founded": "2021-09-10", "founders": ["person:tobie", "person:jaime"],
    # "tags": ["big data", "database"]};
    print(db.person.insert(data))

    bulk = [{'id': "person:jaime", 'name': "Jaime", 'surname': "Morgan Hitchcock"},
            {'id': "person:tobie", 'name': "Tobie", 'surname': "Morgan Hitchcock"}]
    # INSERT INTO person [{"id": "person:jaime", "name": "Jaime", "surname": "Morgan Hitchcock"},
    # {"id": "person:tobie", "name": "Tobie", "surname": "Morgan Hitchcock"}];
    print(db.person.insert(bulk))

    # INSERT INTO company (name, founded) VALUES ('SurrealDB', '2021-09-10');
    print(db.table("company").insert(("name", "founded"), ('SurrealDB', '2021-09-10')))

    # INSERT INTO company (name, founded) VALUES ('Acme Inc.', '1967-05-03'), ('Apple Inc.', '1976-04-01');
    print(db.table("company").insert(("name", "founded"), ('Acme Inc.', '1967-05-03'), ('Apple Inc.', '1976-04-01')))

    # INSERT INTO company (name, url) VALUES ('Salesforce', 'salesforce.com') ON DUPLICATE KEY UPDATE tags += 'crm';
    print(db.table("company").insert(("name", "url"), ('Salesforce', 'salesforce.com')).on_duplicate("tags += 'crm'"))

    # here we're using another statement(select) to insert
    select = db.temperature.select().where("city = 'San Francisco'")
    # INSERT INTO recordings_san_francisco (SELECT * FROM temperature WHERE city = 'San Francisco');
    print(db.table("recordings_san_francisco").insert(select))
