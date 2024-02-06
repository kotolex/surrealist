from surrealist import Surreal

surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("root", "root"))
with surreal.connect() as connection:
    john = connection.create("person", {"first_name": "John", "second_name": "Doe", "age": 21, "active": True})
    jane = connection.create("person", {"first_name": "Jane", "second_name": "Doe", "age": 19})
    john_id = john.result["id"]  # create will return a dict in result field
    jane_id = jane.result["id"]
    select = connection.select(john_id)  # remember it now looks like person:id
    assert select.result[0] == john.result  # select will always return list
    result = connection.merge(jane_id, {"active": True})
    assert not result.is_error()  # check for error
    persons = connection.select("person")
    for person in persons.result:
        print(person)
