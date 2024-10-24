from surrealist import Surreal

# Refer here to use it properly
# https://github.com/kotolex/surrealist?tab=readme-ov-file#change-feeds

surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"))
with surreal.connect() as connection:
    connection.query('CREATE reading set story = "long long time ago";')  # create change on reading table
    dt = "d'2024-02-06T12:25:48.700483Z'"  # assume this moment is AFTER foo and reading were created
    res = connection.show_changes('reading', dt)
    print(res.result)
    # [{'changes': [{'update': {'id': 'reading:w0useg3n9bkne6mei63f', 'story': 'long long time ago'}}]
