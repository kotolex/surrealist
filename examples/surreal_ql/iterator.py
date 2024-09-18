from surrealist import Database

# We can use iterators for select queries
# refer to: https://github.com/kotolex/surrealist/tree/master?tab=readme-ov-file#iteration-on-select
with Database("http://127.0.0.1:8000", 'test', 'test', credentials=("user_db", "user_db")) as db:  # connects to Database
    iterator = db.table("user").select().iter(limit=20)  # get an iterator, nothing executes on this line
    for result in iterator:  # here, where actions actually start
        print(result.count())  # just print count of results, but you cand do anything here

    # we can use iterator with next method
    iterator = db.table("user").select().iter(limit=100)  # get an iterator, nothing executes on this line
    while True:
        res = next(iterator)  # here, only one query performs at a time, then paused
        if res.count() < 100:  # if we get less than 100, it is mean - no more records
            break
