from surrealist import Database

# we connect to a database via Database object
with Database("http://127.0.0.1:8000", 'test', 'test', ('root', 'root')) as db:
    # we need some tables to work with, it is OK if tables not exist yet
    author = db.table("author")
    book = db.table("book")
    counter = db.table("counter")
    # we create queries here but not run it!
    create_author = author.create().content({"name": "john", "id": "john"})
    create_book = book.create().content({"title": "Title", "author": "author:john"})  # book will relate to author
    counter_inc = counter.update("author_count").set("count +=1")  # increment counter

    transaction = db.transaction([create_author, create_book, counter_inc])
    print(transaction)
    # BEGIN TRANSACTION;
    #
    # CREATE author CONTENT {"name": "john", "id": "john"};
    # CREATE book CONTENT {"title": "Title", "author": "author:john"};
    # UPDATE counter:author_count SET count +=1;
    #
    # COMMIT TRANSACTION;

    print(transaction.run().result)
    # [{'result': [{'id': 'author:john', 'name': 'john'}], 'status': 'OK', 'time': '278.709µs'},
    # {'result': [{'author': 'author:john', 'id': 'book:5tyy73v17xqdk24yvj3c', 'title': 'Title'}],
    # 'status': 'OK', 'time': '53.875µs'},
    # {'result': [{'count': 1, 'id': 'counter:author_count'}], 'status': 'OK', 'time': '53.667µs'}]
