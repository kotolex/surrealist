# README #

Surrealist is a Python tool to work with awesome [SurrealDB](https://docs.surrealdb.com/docs/intro) (support for latest version 2.0.0)

It is **synchronous** and **unofficial**, so if you need async AND/OR official client, go [here](https://github.com/surrealdb/surrealdb.py)

Works and tested on Ubuntu, macOS, Windows 10, can use python 3.8+ (including python 3.12)

#### Key features: ####

 * only one small dependency (websocket-client), no need to pull a lot of libraries to your project
 * fully documented
 * well tested (on the latest Ubuntu, macOS and Windows 10)
 * fully compatible with the latest version of SurrealDB (2.0.0), including [live queries](https://surrealdb.com/products/lq) and [change feeds](https://surrealdb.com/products/cf)
 * debug mode to see all that goes in and out if you need (using standard logging)
 * iterator to handle big select queries
 * QL-builder to explore, generate and use SurrealDB queries (explain, transaction etc.)
 * connections pool for use at a high load
 * http or websocket transport to use
 * always up to date with SurrealDB features and changes


### Installation ###

Via pip:

`pip install surrealist`

### Before you start ###
Please make sure you install and start SurrealDB, you can read more [here](https://docs.surrealdb.com/docs/installation/overview)

**Attention!** SurrealDB version 2.0.0 has some breaking changes, so we have to inherit some of them, and you cannot use surrealist version 1.0.0 to work with
Surreal DB version 1.5.3 or earlier. Please consider table to choose a version:

|     SurrealDB version     |     2.0.0      | 1.5.0+   | 1.4.0+   | 1.3.0+   | 1.2.0+   | 1.1.1+   |
|:-------------------------:|:--------------:| :---: |----------|----------|----------|----------|
|    Surrealist version     |     1.0.0      | 0.5.3   | 0.4.2+   | 0.3.1+   | 0.2.10+  | 0.2.3+   |
|      Python versions      |    3.8-3.12    |     3.8-3.12    | 3.8-3.12 | 3.8-3.12 | 3.8-3.12 | 3.8-3.12 |



You can find a lot of examples [here](https://github.com/kotolex/surrealist/tree/master/examples)

A good place to start is connect examples [here](https://github.com/kotolex/surrealist/tree/master/examples/connect.py)

## Transports ##
First of all, you should know that SurrealDB can work with websocket or http "transports", we chose to support both transports here, 
but websockets is preferred and default one. Websockets can use live queries and other cool features.
Each transport has functions it cannot use by itself (in a current SurrealDB version)

**Http-transport cannot:**
 - invalidate, authenticate session
 - create or kill a live query

**Websocket-transport cannot:**
 - import or export data (you should use http connection or cli tools for that)
 - import or export ML files (you should use http connection or cli tools for that)

Transports use a query where it is possible for compatibility, in all other cases if you use these methods on transports -CompatibilityError will be raised


## Connect to SurrealDB ##
All you need is url of SurrealDB and sometimes a few more data to connect

**Example 1**

In this example, we explicitly show all parameters, but remember many of them are optional

```python
from surrealist import Surreal

# we create a surreal object, it can be used to create one or more connections with websockets (use_http=False)
# with timeout 10 seconds
surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"),
                  use_http=False, timeout=10)
print(surreal.is_ready())  # prints True if server up and running on that url
print(surreal.version())  # prints server version
```
**Note:** create of a Surreal object does not attempt any connections or other actions, just store parameters for future use

Calls of **is_ready()**, **health()** or **version()** on Surreal objects are for server checks only, these not validate or check your namespace, database or credentials.

### Parameters ###

**url** - url of SurrealDB server, if you are sure you will use websocket connection - you can use url like ws://127.0.0.1:8000/rpc, but http will work fine too, even for websockets.
So, you can simply use http://127.0.0.1:8000, it will be transform to ws://127.0.0.1:8000/rpc under the hood.
If your url is differed - specify url in ws(s) format

But if you will use ws(s) format, a Surreal object will try to predict http url too; it is important for status and version checks.
For example for wss://127.0.0.1:9000/some/rps predicted http url will be https://127.0.0.1:9000/

**namespace** - name of the namespace, it is optional, but if you use it, you should specify a database too

**database** - name of the database, it is optional, but if you use it, you should specify namespace too

**credentials** - optional, pair(tuple) of username and password for SurrealDB

**use_http** - optional, False by default, flag of using websockets or http transport, False mean using websocket, specify True if you want to use http transport

**timeout** - optional, 5 seconds by default, it is time in seconds to wait for responses and messages, time for trying to connect to SurrealDB


**Example 2**

In this example, we do not use default(optional) parameters

```python
from surrealist import Surreal

# we create a surreal object, it can be used to create one or more connections with websockets
# with timeout 15 seconds
surreal = Surreal("http://127.0.0.1:8000")
print(surreal.is_ready())  # prints True if server up and running on that url
print(surreal.version())  # prints server version
```
## Context managers and close ##
You should always close created connections, when you do not need them anymore, the best way to do it is via context manager

**Example 3**

```python
from surrealist import Surreal

surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"))
with surreal.connect() as ws_connection:  # create context manager, it will close connection for us
    result = ws_connection.select("person")  # select from db
    print(result)  # print result
# here connection is closed
```
You can do the same by itself:

**Example 4**

```python
from surrealist import Surreal

surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"))
ws_connection = surreal.connect()  # open connection
result = ws_connection.select("person")  # select from db
print(result)  # print result
ws_connection.close()  # explicitly close connection
# after closing, we cannot use connection anymore, if you need one - create one more connection with a surreal object
```

## Methods and Query Language ##
Before you go with surrealist, please [check](https://docs.surrealdb.com/docs/surrealql/overview)

You can find basic examples [here](https://github.com/kotolex/surrealist/tree/master/examples)

QL-builder is a simple, convenient way to create queries, validate them and run it against SurealDB. 
It is simple, readable and can be the way to learn QL

**Example 5**

```python
from surrealist import Database

# connects to Database (it is not connection)
with Database("http://127.0.0.1:8000", 'test', 'test', credentials=("user_db", "user_db")) as db: 
    table = db.table("person") # switch to table level, no problem if it is not exists
    print(table.count()) # 0, table is empty or not exists
    # let's add record
    # real query CREATE person:john SET status = "ACTIVE" RETURN id;
    result = table.create("john").set(status="ACTIVE").returns("id").run() 
    # SurrealResult(id=9eb966a4-02fc-40ea-82ba-825d37254f43, status=OK, result=[{'id': 'person:john'}], 
    # query=CREATE person:john SET status = "ACTIVE" RETURN id;, code=None, time=110.3µs, additional_info={})
    print(result)
    print(table.count()) # now one record
```
You can find QL examples [here](https://github.com/kotolex/surrealist/tree/master/examples/surreal_ql)

One of the main features of QL-builder is that using dot you can see all statements available on each level, 
any modern IDE will show possible statements when you type dot. 
Thanks to this, you can study QL and also gain confidence that you are forming a valid query.

for example
`db.account.select().limit(50).start_at(50)` analog "SELECT * FROM account LIMIT 50 START 50;"

Pay attention — you can use just table name without using table() method `db.person.select()`, 
it is readable and shorter, but in that particular case you will not get IDE suggestions.

So, we recommend using table() method `db.table("person").select()` it is not much bigger, but still readable, 
and you will get help from your IDE

If you cannot form your query with QL, you always can use a raw query via `database.raw_query` or `connection.query`
It is the most efficient way, cause it allows you to do all that is possible if you have permissions.

### Iteration on Select ###

When you expect a lot of data on your select query via QL-builder, you should consider using iterator, it is a simple, lazy and common way to use in python.

Iterator can be used with **next** method or in **for** statement

**Example 6**

```python
from surrealist import Database

with Database("http://127.0.0.1:8000", 'test', 'test', credentials=("user_db", "user_db")) as db: # connects to Database
    iterator = db.table("user").select().iter(limit=20) # get an iterator, nothing executes on this line
    for result in iterator: # here, where actions actually start
        print(result.count()) # just print count of results, but you cand do anything here
```

## Results and RecordID ##
If the method of connection is not raised, it is always returns SurrealResult object on any response of SurrealDB. It was chosen for simplicity.

Please see [examples](https://github.com/kotolex/surrealist/blob/master/examples/result.py)

Here is standard result:
`SurrealResult(id=None, status=OK, result=[{'author': '51ff5faa-d798-4194-93c6-179ce7525a8c', 'id': 'article:⟨51ff5faa-d798-4194-93c6-179ce7525a8c⟩', 'text': '51ff5faa-d798-4194-93c6-179ce7525a8c', 'title': '51ff5faa-d798-4194-93c6-179ce7525a8c'}], query=None, code=None, time=77.25µs, additional_info={})`

Here is standard error:
`SurrealResult(id=ca3eface-9287-4092-a198-4f91ed27a010, status=ERR, result={'code': -32000, 'message': 'There was a problem with authentication'}, query=None, code=None, time=None, additional_info={})
`

You can always check for error using is_error() method

```python
if result.is_error():
    raise ValueError("Got error")
```

Besides, a result object has helper methods **is_empty**, **id**, **ids**, **get**, **first**, **last** to work with response of SurrealDB.

You need to read this on SurrealDB recordID: https://docs.surrealdb.com/docs/surrealql/datamodel/ids

For support and compatibility reasons, there are two ways to specify recordID in method.
Let's consider CREATE method, for example:
```python
# all the same
ws_connection.create("person:john", {"name":"John Doe"})
ws_connection.create("person", {"name":"John Doe"}, record_id="john")
```
These function calls are the same, so, as you can see, recordID can be specified:
 - in table name after colon, for example, "person:john"
 - in record_id argument

**Attention!** Using recordID 2 times in one method will cause error on SurrealDB side, for example
`ws_connection.select("person:john", record_id="john")` is invalid call, id should be specified only once.

**Important note:** uuid-type recordID, like 8424486b-85b3-4448-ac8d-5d51083391c7 should be specified with "`" backticks, for example
```python
ws_connection.select("person:`8424486b-85b3-4448-ac8d-5d51083391c7`")
ws_connection.select("person", record_id="`8424486b-85b3-4448-ac8d-5d51083391c7`")
```
otherwise, you can't find your record

**Important note for UTF-8:** record_id can have only A-Z, a-z letters, if you want to use any other UTF-8 letters you have to use backticks "`"!

**Note:** record_id parameter of methods is only concatenated with table_name and colon, so
ws_connection.select("person", record_id="john") under the hood became
ws_connection.select("person:john"), but no other logic performed. Do not expect we specified "`" for you.


## Logging and Debug mode ##
As it was said, if you need to debug something, stuck in some problem or just want to know all about data between you and SurrealDB, you can use standard logging.
All library logs will contain "surrealist" prefix in logs. You, as a developer, should choose proper handlers, formats, filters etc.
Surrealist does not use root logger, does not use any handlers and uses only DEBUG, INFO and ERROR level for its events.

For example

**Example 7**

```python
from logging import basicConfig, INFO, DEBUG

from surrealist import Surreal, LOG_FORMAT  # LOG_FORMAT is used for simplicity, you can use your own

basicConfig(format=LOG_FORMAT, level=INFO)  # we specify a format and level of events to catch

surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"))
with surreal.connect() as connection:
    res = connection.create("article", {"author": "John Doe", "title": "In memoriam", "text": "text"})
```
if you run it, you get in the console:
```
2024-05-29 15:59:41,759 : Thread-1 : websocket : INFO : Websocket connected
2024-05-29 15:59:41,762 : MainThread : surrealist.connections.websocket : INFO : Operation: SIGNIN. Data: {'user': 'user_db', 'pass': '******', 'NS': 'test', 'DB': 'test'}
2024-05-29 15:59:41,788 : MainThread : surrealist.connections.websocket : INFO : Got result: SurrealResult(id=c3fcebbc-359f-47d0-822b-a4ad8043f64b, status=OK, result=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpYXQiOjE3MTY5ODAzODEsIm5iZiI6MTcxNjk4MDM4MSwiZXhwIjoxNzE2OTgzOTgxLCJpc3MiOiJTdXJyZWFsREIiLCJqdGkiOiI0YTQyNWFiNy00NGEyLTQ4OGItYjM4MS05YjUyNDQzYTI5OTQiLCJOUyI6InRlc3QiLCJEQiI6InRlc3QiLCJJRC...
2024-05-29 15:59:41,788 : MainThread : surrealist.connections.websocket : INFO : Connected to ws://127.0.0.1:8000/rpc, params: {'NS': 'test', 'DB': 'test'}, credentials: ('root', '******'), timeout: 15
2024-05-29 15:59:41,788 : MainThread : surrealist.connections.websocket : INFO : Operation: CREATE. Path: article, data: {'author': 'John Doe', 'title': 'In memoriam', 'text': 'text'}
2024-05-29 15:59:41,794 : MainThread : surrealist.connections.websocket : INFO : Got result: SurrealResult(id=b307d67f-b01b-4b71-a319-906fa17b8c72, status=OK, result=[{'author': 'John Doe', 'id': 'article:b44tdiiyb8jw6mcn1tzs', 'text': 'text', 'title': 'In memoriam'}], query=None, code=None, time=None, additional_info={})
2024-05-29 15:59:41,794 : MainThread : surrealist.connection : INFO : The connection was closed
```
but if in the example above (example 7) you choose "DEBUG" for level, you will see all, including low-level clients' data:
```
2024-05-29 16:03:58,438 : MainThread : surrealist.clients.websocket : DEBUG : Connecting to ws://127.0.0.1:8000/rpc
2024-05-29 16:03:58,445 : Thread-1 : websocket : INFO : Websocket connected
2024-05-29 16:03:58,458 : MainThread : surrealist.clients.websocket : DEBUG : Connected to ws://127.0.0.1:8000/rpc, timeout is 15 seconds
2024-05-29 16:03:58,458 : MainThread : surrealist.connections.websocket : INFO : Operation: SIGNIN. Data: {'user': 'user_db', 'pass': '******', 'NS': 'test', 'DB': 'test'}
2024-05-29 16:03:58,458 : MainThread : surrealist.clients.websocket : DEBUG : Send data: {"id": "1d5758bb-0879-4d8d-9e14-37c9117669a3", "method": "signin", "params": [{"user": "root", "pass": "******", "NS": "test", "DB": "test"}]}
2024-05-29 16:03:58,484 : Thread-1 : surrealist.clients.websocket : DEBUG : Get message b'{"id":"1d5758bb-0879-4d8d-9e14-37c9117669a3","result":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpYXQiOjE3MTY5ODA2MzgsIm5iZiI6MTcxNjk4MDYzOCwiZXhwIjoxNzE2OTg0MjM4LCJpc3MiOiJTdXJyZWFsREIiLCJqdGkiOiIwODhhMWY0My04YzY3LTQ5NjYtYTdjNC02ZGI5NjA0MGNkYmIiLCJOUyI6InRlc3QiLCJEQiI6InRlc3QiLCJJRCI6InJvb3QifQ.1pSbJ'...
2024-05-29 16:03:58,484 : MainThread : surrealist.connections.websocket : INFO : Got result: SurrealResult(id=1d5758bb-0879-4d8d-9e14-37c9117669a3, status=OK, result=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpYXQiOjE3MTY5ODA2MzgsIm5iZiI6MTcxNjk4MDYzOCwiZXhwIjoxNzE2OTg0MjM4LCJpc3MiOiJTdXJyZWFsREIiLCJqdGkiOiIwODhhMWY0My04YzY3LTQ5NjYtYTdjNC02ZGI5NjA0MGNkYmIiLCJOUyI6InRlc3QiLCJEQiI6InRlc3QiLCJJRC...
2024-05-29 16:03:58,484 : MainThread : surrealist.connections.websocket : INFO : Connected to ws://127.0.0.1:8000/rpc, params: {'NS': 'test', 'DB': 'test'}, credentials: ('root', '******'), timeout: 15
2024-05-29 16:03:58,484 : MainThread : surrealist.connections.websocket : INFO : Operation: CREATE. Path: article, data: {'author': 'John Doe', 'title': 'In memoriam', 'text': 'text'}
2024-05-29 16:03:58,484 : MainThread : surrealist.clients.websocket : DEBUG : Send data: {"id": "9bbc90d7-d6dc-4a51-ad97-b765e6b09131", "method": "create", "params": ["article", {"author": "John Doe", "title": "In memoriam", "text": "text"}]}
2024-05-29 16:03:58,490 : Thread-1 : surrealist.clients.websocket : DEBUG : Get message b'{"id":"9bbc90d7-d6dc-4a51-ad97-b765e6b09131","result":[{"author":"John Doe","id":"article:72duj8mef1s97c67dv38","text":"text","title":"In memoriam"}]}'
2024-05-29 16:03:58,491 : MainThread : surrealist.connections.websocket : INFO : Got result: SurrealResult(id=9bbc90d7-d6dc-4a51-ad97-b765e6b09131, status=OK, result=[{'author': 'John Doe', 'id': 'article:72duj8mef1s97c67dv38', 'text': 'text', 'title': 'In memoriam'}], query=None, code=None, time=None, additional_info={})
2024-05-29 16:03:58,491 : MainThread : surrealist.connection : INFO : The connection was closed
2024-05-29 16:03:58,491 : Thread-1 : surrealist.clients.websocket : DEBUG : Close connection to ws://127.0.0.1:8000/rpc
2024-05-29 16:03:58,491 : MainThread : surrealist.clients.websocket : DEBUG : Client is closed connection to ws://127.0.0.1:8000/rpc
```

**Note:** passwords and auth information always masked in logs. If you still see it in logs - please, report an issue


## Live Query ##
Live queries let you subscribe to events of desired table when changes happen—you get notification as a simple result or in DIFF format

About live query: https://surrealdb.com/products/lq

Using live select: https://surrealdb.com/docs/surrealdb/surrealql/statements/live

About DIFF (jsonpatch): https://jsonpatch.com

LQ can work only with websockets, you have to provide a callback function to call on any event.

Callback should have signature `def any_name(param: Dict) -> None`, so it will be called with python dictionary as only argument

**Note 1:** if your connection was interrupted or closed, LQ will disappear, and you need to recreate it

**Note 2:** LQ only produces events which happen after the creation of this LQ

**Note 3:** LQ is associated with connection, where it was created, if you have two or more connections, LQ will depend only on one, 
and will disappear on connection close, even if other connections are still active

**Note 4:** LQ is stop working after calling REMOVE TABLE for table it listens on. This will be fixed in future SurrealDB versions

**Example 8**

```python
from time import sleep
from surrealist import Surreal


# you need callback, a function which will get dictionary and do something with it
def call_back(response: dict) -> None:
    print(response)


# you need websockets for a live query
surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"))
with surreal.connect() as connection:
    res = connection.live("person", callback=call_back)  # here we subscribe on person table
    live_id = res.result  # live_id is a LQ id, we need it to kill a query
    connection.create("person", {"name": "John", "surname": "Doe"})  # here we create an event
    sleep(0.5)  # sleep a little cause need some time to get a message back
```
in console, you will get:
`{'result': {'action': 'CREATE', 'id': 'c2c8952b-b2bc-4d3a-aa68-4609f5818d7c', 'result': {'id': 'person:dik1sm50xr2d5mc7fysi', 'name': 'John', 'surname': 'Doe'}}}`

**Example 9**

```python
from time import sleep
from surrealist import Surreal


# you need callback, a function which will get dictionary and do something with it
def call_back(response: dict) -> None:
    print(response)


# you need websockets for a live query
surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"))
with surreal.connect() as connection:
    # here we subscribe on person table and specify we need DIFF
    res = connection.live("person", callback=call_back, return_diff=True)
    live_id = res.result  # live_id is a LQ id, we need it to kill a query
    connection.create("person", {"name": "John", "surname": "Doe"})  # here we create an event
    sleep(0.5)  # sleep a little cause need some time to get a message back
    connection.kill(live_id)  # we kill LQ, no more events to come
```
in console, you will get:
`{'result': {'action': 'CREATE', 'id': '54a4dd0b-0008-46f4-b4e6-83e466cb4141', 'result': [{'op': 'replace', 'path': '/', 'value': {'id': 'person:fhglyrxkit3j0fnosjqg', 'name': 'John', 'surname': 'Doe'}}]}}`

If you do not need LQ anymore, call KILL method, with live_id

You can use a custom live query if you need, it lets you use filters and conditions, as refer [here](https://surrealdb.com/docs/surrealdb/surrealql/statements/live#filter-the-live-query)

**Example 10**

```python
from time import sleep

from surrealist import Surreal


# you need callback, a function which will get dictionary and do something with it
def call_back(response: dict) -> None:
    print(response)


# you need websockets for a live query
surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"))
with surreal.connect() as connection:
    # here we subscribe and specify a custom query for persons
    res = connection.custom_live("LIVE SELECT * FROM ws_person WHERE age > 18;", callback=call_back)
    live_id = res.result  # live_id is a LQ id, we need it to kill a query in future
    # here we create 2 records but only the second one is what we look for
    connection.create("ws_person", {"age": 16, "name": "Jane"})  # Jane is too young for us :)
    connection.create("ws_person", {"age": 28, "name": "John"})  # John older than 18, so wee need this event
    sleep(0.5)  # sleep a little cause need some time to get a message back
    connection.kill(live_id)  # we kill LQ, no more events to come
```

in console, you will get:
`{'result': {'action': 'CREATE', 'id': '1f57f2de-354a-43ba-8f39-57000944707c', 'result': {'age': 28, 'id': 'ws_person:awot8zdkg3mqj4wymq8c', 'name': 'John'}}}`

Pay attention — there is no info about Jane in events we get from LQ, cause Jane is younger than 18.

Same example with QL-builder:

```python
from time import sleep

from surrealist import Database


# you need callback, a function which will get dictionary and do something with it
def call_back(response: dict) -> None:
    print(response)


# you need websockets for a live query
with Database("http://127.0.0.1:8000", 'test', 'test', credentials=("user_db", "user_db")) as db:
    table = db.table("ws_person")
    # here we subscribe and specify a custom query for persons
    result = table.live(callback=call_back).where("age > 18").run()
    live_uid = result.result # live_id is a LQ id, we need it to kill a query in future
    # here we create 2 records but only the second one is what we look for
    table.create().content({"age": 16, "name": "Jane"}).run()  # Jane is too young for us :)
    table.create().content({"age": 28, "name": "John"}).run()  # John older than 18, so wee need this event
    sleep(0.1)
    result = table.kill(live_uid)  # we kill LQ, no more events to come
```


## Change Feeds ##
Changes in the database, such as creating, updating, or deleting, are recorded and played back in another channel. 
This channel functions as a stream of messages.

Change Feeds are great for ensuring accurate order and consistent replication of tables or databases. They also provide 
immediate updates on any changes made.

Read here: https://surrealdb.com/blog/unlocking-streaming-data-magic-with-surrealdb-live-queries-and-change-feeds

Read here: https://surrealdb.com/products/cf

Under the hood: https://docs.surrealdb.com/docs/surrealql/statements/show

Changes Feed works both for http and websockets!

Let's set up everything:
```
DEFINE TABLE reading CHANGEFEED 1d;
```

**Note:** date and time of your requests should be strict AFTER date and time of creating `reading`

**Example 11**

```python
from surrealist import Surreal


surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("user_db", "user_db"))
with surreal.connect() as connection:
    # Again, 2024-02-06T10:48:08.700483Z - is a moment AFTER the table was created
    res = connection.query('SHOW CHANGES FOR TABLE reading SINCE "2024-02-06T10:48:08.700483Z" LIMIT 10;')
    print(res.result) # it will be [] cause no events happen
    # now we add one record
    connection.query('CREATE reading set story = "long long time ago";')    
    # check again
    res = connection.query('SHOW CHANGES FOR TABLE reading SINCE "2024-02-06T10:48:08.700483Z" LIMIT 10;')
    print(res.result) 
```
in the console, you will see
`[{'changes': [{'update': {'id': 'reading:w0useg3n9bkne6mei63f', 'story': 'long long time ago'}}], 'versionstamp': 851968}]`

Same example via QL-builder:

**Example 12**

```python
from datetime import datetime, timezone
from surrealist import Database, to_surreal_datetime_str


with Database("http://127.0.0.1:8000", 'test', 'test', credentials=("user_db", "user_db")) as db:
    tm = to_surreal_datetime_str(datetime.now(timezone.utc)) # Again, here is a moment AFTER the table was created
    res = db.table("reading").show_changes().since(tm).run()
    print(res.result) # it will be [] cause no events happen
    # now we add one record
    db.table("reading").create().set(story="long long time ago").run()
    res = db.table("reading").show_changes().since(tm).run()
```


## Threads and thread-safety ##
Remember, SurrealDB is "surreally" fast, so first make sure you need to use multiple threads to work with it, because in many situations
one thread is enough to do the job. Do not fall to premature optimizations. 

All objects, including connections, statements, database are thread-safe, so you can use all library features in different threads.

This library was made for using in multithreading environments, remember some rules of thumb:
 - if you work with only one server of SurrealDB, you need only one Surreal object
 - one Connection/Database object represents exactly one connection (websocket or http) with DB
 - it is OK to use connection in different threads, but it can be your bottleneck, as there is only one connection to DB
 - with many queries and high load, you should consider using more than one connection, but not too many of them. The number of connections equal to the number of CPU-cores is the best choice
 - remember to properly close connections

## Connections Pool ##
And again, please, do not fall to premature optimizations, when working with SurrealDB. But if you consider or expect a high load and/or a lot of 
threads, which are use SurrealDB, you can use DatabaseConnectionsPool. It can be used exactly like a Database object, the main difference — you 
can specify minimum and maximum connections to use. Under high load, when a lot of data goes in and out in a lot of threads - a pool object can 
make job faster and effectively, than one common connection.

On start pool will create minimum number of connections, and on a big load will be creating more and more connections until reach the maximum of them.
By default, the minimum number is equal to CPU cores count for the system. 
So any incoming request from your application will use the first non-busy connection it gets from the pool.

Pay attention — new connections can be created, but old connections never be closed until the pool will be closed, so the number of connections can grow, 
but never can shrink. It is because of Live Queries, as you remember: LQ always linked to connection, so if connection will be closed, LQ stop working.

**Example 13**

```python
from surrealist import DatabaseConnectionsPool


with DatabaseConnectionsPool("http://127.0.0.1:8000", 'test', 'test', credentials=("user_db", "user_db"), min_connections=10, 
                             max_connections=40) as db: # create pool, it creates 10 connections on start
    make_something_with_a_lot_of_threads_or_data(db) # use pool everywhere we need as a simple Database object
```

**Note:** DatabaseConnectionsPool is NOT a singleton, it allows creating as many pools as you like, for example, for different databases or namespaces. 
It is your job as a developer to limit number of pools created in your application

**Important note:** for many and maybe the most cases, one shared connection is enough to do the job. Test it and make sure you really need a connection pool.

## Recursion and JSON in Python ##
SurrealDb has _"no limit to the depth of any nested objects or values within"_, but in Python we have a recursion limit and
standard json library (and str function) use recursion to load and dump objects, so if you will have deep nesting in your objects - 
you can get RecursionLimitError. 

The best choice here is to rethink your schema and objects, because you probably do 
something wrong with such a high level of nesting. 

Second choice — increase recursion limit in your system with
```python
import sys
sys.setrecursionlimit(10_000)
```
## Examples ##
You can find a lot of examples [here](https://github.com/kotolex/surrealist/tree/master/examples)

### Contacts ###
Mail me at farofwell@gmail.com