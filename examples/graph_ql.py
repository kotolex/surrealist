from surrealist import Surreal

# Refer here to use it properly
# https://surrealdb.com/docs/surrealdb/querying/graphql

# Make sure to use HTTP transport and run DEFINE CONFIG GRAPHQL AUTO; before any query

surreal = Surreal("http://127.0.0.1:8000", credentials=('root', 'root'), use_http=True)
with surreal.connect() as connection:
    connection.use("test", "test")
    res = connection.graphql({"query": "{ author { id } }"}, pretty=False)
    print(res.result)
    # {'data': {'author': [{'id': 'author:twazxl'}]}}