from time import sleep

from surrealist import Surreal

# Please, read https://github.com/kotolex/surrealist?tab=readme-ov-file#live-query

# you need callback, a function which will get dictionary and do something with it
def call_back(response: dict) -> None:
    print(response)


# you need websockets for a live query
surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"))
with surreal.connect() as connection:
    # here we subscribe on person table
    res = connection.live("person", callback=call_back)
    live_id = res.result  # live_id is a LQ id, we need it to kill a query
    connection.create("person", {"name": "John", "surname": "Doe"})  # here we create an event
    sleep(0.5)  # sleep a little cause need some time to get a message back
    # will be printed:
    # {'result': {'action': 'CREATE', 'id': 'cf204055-967a-414b-908b-eb607a571828', 'result':
    # {'id': 'person:seg3juu35udmb3qjajce', 'name': 'John', 'surname': 'Doe'}}}
    connection.kill(live_id)  # we kill LQ, no more events to come

    # DIFF EXAMPLE

    # here we subscribe on person table and specify we need DIFF
    res = connection.live("person", callback=call_back, return_diff=True)
    live_id = res.result  # live_id is a LQ id, we need it to kill a query
    connection.create("person", {"name": "Jane", "surname": "Doe"})  # here we create an event
    sleep(0.5)  # sleep a little cause need some time to get a message back
    # will be printed:
    # {'result': {'action': 'CREATE', 'id': '821cd1f2-56ac-4756-a815-58865b5f71d1', 'result': [{'op': 'replace',
    # 'path': '/', 'value': {'id': 'person:g5hgoe4vgko9i2jdtzfz', 'name': 'Jane', 'surname': 'Doe'}}]}}
    connection.kill(live_id)  # we kill LQ, no more events to come

    # CUSTOM EXAMPLE

    # here we subscribe and specify a custom query for persons
    res = connection.custom_live("LIVE SELECT * FROM ws_person WHERE age > 18;", callback=call_back)
    live_id = res.result  # live_id is a LQ id, we need it to kill a query in future
    # here we create two records but only the second one is what we look for
    connection.create("ws_person", {"age": 16, "name": "Jane"})  # Jane is too young for us :)
    connection.create("ws_person", {"age": 28, "name": "John"})  # John older than 18, so wee need this event
    sleep(0.5)  # sleep a little cause need some time to get a message back
    # will be printed:
    # {'result': {'action': 'CREATE', 'id': 'ba79288c-11b2-459f-975f-79b52f283b1d', 'result': {'age': 28, 'id':
    # 'ws_person:5c43edfsoszzpg842182', 'name': 'John'}}}
    connection.kill(live_id)  # we kill LQ, no more events to come
