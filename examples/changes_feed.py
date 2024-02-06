from surrealist import Surreal

# Refer here to use it properly
# https://github.com/kotolex/py_surreal?tab=readme-ov-file#change-feeds

surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("root", "root"))
with surreal.connect() as connection:
    connection.query('CREATE reading set story = "long long time ago";')  # create change on reading table
    dt = "2024-02-06T12:25:48.700483Z"  # assume this moment is AFTER foo and reading were created
    res = connection.show_changes('reading', "2024-02-06T12:25:48.700483Z")
    print(res.result)
    # [{'changes': [{'update': {'id': 'reading:w0useg3n9bkne6mei63f', 'story': 'long long time ago'}}]
