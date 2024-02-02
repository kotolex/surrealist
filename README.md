# README #

Py_Surreal is a Python tool to work with awesome [SurrealDB](https://docs.surrealdb.com/docs/intro)

It is blocking and **unofficial**, so if you need async AND/OR official client go [here](https://github.com/surrealdb/surrealdb.py)

Key features:

 * only one small dependency (websocket-client), no need to pull a lot of libraries to yor project
 * fully documented
 * fully tested (on latest Ubuntu, macOS and Windows 10)
 * fully compatible with latest version of SurrealDB (1.1.1), including live queries
 * debug mode to see all that goes in and out, if you need
 * http or websocket transport to use

More to come:
 * connections pool
 * additional features (explain, count etc.)


### Installation ###

Via pip:

`pip install py_surreal` # not working right now

### Before you start ###
Please, make sure you install and start SurrealDB, you can read more [here](https://docs.surrealdb.com/docs/installation/overview)

### Connect to SurrealDB ###
First of all, you should know that SurrealDB can work on websocket or http "transports", we chose to support both transports, 
but websockets id preferred and default one. Web



```python
from py_surreal import Surreal

surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("root", "root"))
print(surreal.is_ready()) # prints True if server p and running on that url
print(surreal.version()) # prints server version
```

### Recursion and JSON in Python ###
SurrealDb has _"no limit to the depth of any nested objects or values within"_, but in Python we have a recursion limit and
standard json library use recursion to load and dump objects, so if you will have deep nesting in your objects - 
you can get RecursionLimitError. The best choice here is to rethink your schema and objects, because you probably do 
something wrong with such nesting. Second choice - increase recursion limit in your system with
```python
import sys
sys.setrecursionlimit(10_000)
```
