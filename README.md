# README #

Py_Surreal is a Python tool to work with awesome [SurrealDB](https://docs.surrealdb.com/docs/intro)

It is blocking and **unofficial**, so if you need async AND/OR official client go [here](https://github.com/surrealdb/surrealdb.py)

Key features:

 * only one small dependency (websocket-client), no need to pull a lot of libraries to yor project
 * fully documented
 * fully tested (on latest Ubuntu, macOS and Windows 10)
 * fully compatible with latest version of SurrealDB (1.1.1), including live queries
 * debug mode to see all that goes in and out, if you need
 * http or websocket clients to use

More to come:
 * connections pool
 * additional features (explain, count etc.)


### Installation ###

Via pip:

`pip install py_surreal` # not working right now

### Before you start ###
Please, make sure you install and start SurrealDB, you can read nore [here](https://docs.surrealdb.com/docs/installation/overview)





```python
from py_surreal import Surreal

surreal = Surreal("http://127.0.0.1:8000", namespace="test", database="test", credentials=("root", "root"))
print(surreal.is_ready()) # prints True if server p and running on that url
print(surreal.version()) # prints server version
```
