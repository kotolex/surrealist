from surrealist import Database

with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db'), use_http=True) as db:
    # on database object we can DEFINE USER

    # DEFINE USER new_user ON DATABASE PASSWORD '123456' ROLES OWNER;
    print(db.define_user("new_user").password("123456").role_owner())
    # DEFINE USER IF NOT EXISTS new_user ON DATABASE PASSWORD '123456' ROLES EDITOR;
    print(db.define_user("new_user").if_not_exists().password("123456").role_editor())
    # DEFINE USER OVERWRITE new_user ON DATABASE PASSWORD '123456' ROLES VIEWER;
    print(db.define_user("new_user").overwrite().password("123456").role_viewer())
    # DEFINE USER username ON ROOT PASSWORD '123456' ROLES OWNER DURATION FOR SESSION 15m, FOR TOKEN 5s;
    print(db.define_user("username").password("123456").role_owner().duration_session("15m").duration_token("5s"))
    # DEFINE USER example ON DATABASE PASSWORD 'example' ROLES VIEWER COMMENT "example";
    print(db.define_user("example").password("example").role_viewer().comment("example"))

    # we can remove user
    print(db.remove_user("new_user"))  # REMOVE USER new_user ON DATABASE;
