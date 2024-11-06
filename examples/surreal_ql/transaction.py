from surrealist import Database, RecordId

# we connect to a database via Database object
with Database("http://127.0.0.1:8000", 'test', 'test', credentials=("user_db", "user_db")) as db:
    # we need some tables to work with, it is OK if tables not exist yet
    author = db.table("author")
    book = db.table("book")
    counter = db.table("counter")
    # we create queries here but not run it!
    john = RecordId("johnny", table="author")  # Pay attention how you should use RecordId
    author_count = RecordId("authorcount", table="counter")  # Pay attention how you should use RecordId
    create_author = author.create().content({"name": "john", "id": john})
    create_book = book.create().content({"title": "Title", "author": john})  # book will relate to author
    counter_inc = counter.upsert(author_count).set("count +=1")  # increment counter

    # on transactions see https://docs.surrealdb.com/docs/surrealql/transactions
    transaction = db.transaction([create_author, create_book, counter_inc])
    print(transaction)
    # BEGIN TRANSACTION;
    #
    # CREATE author CONTENT {"name": "john", "id": author:johnny};
    # CREATE book CONTENT {"title": "Title", "author": author:johnny};
    # UPDATE counter:authorcount SET count +=1;
    #
    # COMMIT TRANSACTION;

    print(transaction.run().result)
    # [{'result': [{'id': 'author:johnny', 'name': 'john'}], 'status': 'OK', 'time': '115.9µs'},
    # {'result': [{'author': 'author:johnny', 'id': 'book:0yhz1i69d4ifocips9jk', 'title': 'Title'}], 'status': 'OK', 'time': '312.1µs'},
    # {'result': [{'count': 1, 'id': 'counter:authorcount'}], 'status': 'OK', 'time': '263.4µs'}]
