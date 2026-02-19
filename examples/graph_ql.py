from surrealist import Surreal

# Refer here to use it properly
# https://surrealdb.com/docs/surrealdb/querying/graphql

# Make sure to use HTTP transport and run DEFINE CONFIG GRAPHQL AUTO; before any query

surreal = Surreal("http://127.0.0.1:8000", credentials=('root', 'root'), use_http=True)  # http only!
with surreal.connect() as connection:
    connection.use("test", "test")
    # you need at least 1 table at database
    connection.create("author", {"age": 31, "is_alive": False}, "john")
    connection.query("DEFINE CONFIG GRAPHQL AUTO;")  # you need this for GraphQL to work
    res = connection.graphql({"query": "{ author { id } }"})
    print(res.result)  # {'data': {'author': [{'id': 'author:john'}]}}
