from surrealist import Surreal

# using websockets by default
surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("root", "root"))
with surreal.connect() as connection:
    # create 2 records at person table
    john = connection.create("person", {"first_name": "John", "second_name": "Doe", "age": 21, "active": True})
    jane = connection.create("person", {"first_name": "Jane", "second_name": "Doe", "age": 19})
    john_id = john.result["id"]  # create will return a dict in result field
    jane_id = jane.result["id"]
    select = connection.select(john_id)  # remember it now looks like person:id
    assert select.result[0] == john.result  # select will always return list
    # add active field for Jane
    result = connection.merge(jane_id, {"active": True})
    assert not result.is_error()  # check for error
    # get all persons, now they both have active field
    persons = connection.select("person")
    for person in persons.result:
        print(person)
    # delete john
    connection.delete(john_id)
    print(connection.count("person").result)  # 1, only Jane
    # remove whole table with jane in it
    connection.remove_table("person")
    print(connection.db_tables().result)  # should be no person table
