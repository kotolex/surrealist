from surrealist import Surreal, Database

# Below, you can see examples for connecting with different users (root/ namespace/ db)
# Please, pay attention for differs for different user levels

# auth as root user
surreal = Surreal("http://127.0.0.1:8000", credentials=("root", "root"))
with surreal.connect() as ws_connection:
    print(ws_connection.root_info())  # get root info

# auth as namespace user
surreal = Surreal("http://127.0.0.1:8000", namespace="test", credentials=("user_ns", "user_ns"))
with surreal.connect() as ws_connection:
    print(ws_connection.ns_info())  # get namespace info

# auth as db user
surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"))
with surreal.connect() as ws_connection:
    print(ws_connection.db_info())  # get database info

# Note: if you need to work on db level with root or namespace user, you need to explicitly call USE method

# auth as root user and use db level
surreal = Surreal("http://127.0.0.1:8000", credentials=("root", "root"))
with surreal.connect() as ws_connection:
    ws_connection.use("test", "test")
    print(ws_connection.db_info())  # get database info

# auth as namespace user and use db level
surreal = Surreal("http://127.0.0.1:8000", namespace="test", credentials=("user_ns", "user_ns"))
with surreal.connect() as ws_connection:
    ws_connection.use("test", "test")
    print(ws_connection.db_info())  # get database info

# Note 2: to use root or namespace user for Database, you can use connection, but before switch to db level
surreal = Surreal("http://127.0.0.1:8000", credentials=("root", "root"))
with surreal.connect() as ws_connection:
    ws_connection.use("test", "test")
    db = Database.from_connection(ws_connection)
    print(db.info())
