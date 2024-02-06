from surrealist import Surreal

# we have to use http transport for import/export
surreal = Surreal("http://127.0.0.1:8000", "test", "test", credentials=("root", "root"), use_http=True)
with surreal.connect() as connection:
    # first we export data from SurrealDB
    with open("exported.surql", "wt", encoding="UTF-8") as file:
        file.write(connection.export())
    # now, after closing file, we can import it back
    connection.import_data("exported.surql")  # Pay attention - we do not read file by ourselves, just specify name/path
