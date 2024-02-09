from surrealist import Database

with Database("http://127.0.0.1:8000", 'test', 'test', ('root', 'root')) as db:
    print(db.tables())  # [] cause database is empty
    table = db.table("person")  # even there is no such table yes it is OK to switch on it
    print(table.count())  # and even get count of records which is 0 for non-existent table
    # now let's create one record via set operator
    result = table.create().set(name="Alex", age=30, active=True).run()  # pay attention how we use keyword parameters
    print(result.result)  # [{'active': True, 'age': 30, 'id': 'person:wfulrb7dqml3vv7koqnb', 'name': 'Alex'}]
    # now let's create one record via python dict
    result = table.create().content({'name': "Jane", 'age': 22, 'active': True}).run()  # dict now
    print(result.result)  # [{'active': True, 'age': 22, 'id': 'person:x57n30x0n0sd104y235k', 'name': 'Jane'}]

    # let's check all it really in table, we use select all
    result = table.select().run()
    # [{'active': True, 'age': 22, 'id': 'person:84iakx5ptoeljoc4hv07', 'name': 'Jane'},
    # {'active': True, 'age': 30, 'id': 'person:8vivzq48kof6bg5dlc7a', 'name': 'Alex'}]
    print(result.result)
    result = table.select('*').run()  # same result if we use '*'
    print(result.result)
