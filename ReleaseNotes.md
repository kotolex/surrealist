## Release Notes ##
**Version 1.0.0 (compatible with SurrealDB version 2.0.0):**
- both http and ws connections lost signin and signup methods, sign in now happen under the hood on connect
- USE method for http just adds headers "surreal-db" and "surreal-ns" for http-requests
- for root user now you should connect only with credentials and then call use method (see examples)
- you can now specify access method on connecting via a Surreal object
- now use Bearer instead of Basic for Authorization
- database argument is now optional for USE method, but raises for websocket if database is not specified

**Version 0.5.3 (compatible with SurrealDB version 1.5.2):**
- minor fixes for docs and examples

**Version 0.5.2 (compatible with SurrealDB version 1.5.1):**
- now user should use standard logging for debug
- remove log_level arguments for classes
- add LOG_FORMAT for default uses
- fix README

**Version 0.5.1 (compatible with SurrealDB version 1.5.0):**
- add more precise names for INDEX methods (M, TYPE)
- limit allowed options for some methods (TYPE)
- add documentation for index methods
- change BM25 (add docs and parameters)
- fix validate method bug for changefeed
- fix README, examples and tests

**Version 0.5.0 (compatible with SurrealDB version 1.5.0):**
- although INCLUDE ORIGINAL statement exists, it has no effect (SurrealDB team decision for 1.5.0)
- add REBUILD INDEX statement, usable for database and table objects
- improve DEFINE INDEX statement, now it can use MTREE and HNSW indexes
- fix https://github.com/kotolex/surrealist/issues/44 , so now set() and other methods which have optional string and 
keyword-arguments for QL will combine string and kwargs
- add INFO ... STRUCTURE feature as optional, by default, it is not used as it is for internal use of SDB
- add more examples and tests

**Version 0.4.1 (compatible with SurrealDB version 1.4.2):**
- add to_surreal_datetime_str and to_datetime functions, cause SurealDB has a specific datetime format
- minor improvements for documentation, examples and tests

**Version 0.4.0 (compatible with SurrealDB version 1.4.0):**
- code attribute of a result is now containing http status code or error code for websocket
- SHOW CHANGES uses current date-time if not specified, because SHOW does not work without SINCE
- fix live queries url links
- live query for QL is now can use VALUE and custom query
- DEFINE TABLE now can use TYPE and INCLUDE ORIGINAL
- minor improvements for documentation, examples and tests

**Version 0.3.1 (compatible with SurrealDB version 1.3.1):**
- increment default timeout to 15 seconds
- minor improvements for documentation

**Version 0.3.0 (compatible with SurrealDB version 1.3.0):**
- add IF NOT EXISTS for all DEFINE statements
- add IF EXISTS for all REMOVE statements
- improvements for documentation, examples and tests

**Version 0.2.10 (compatible with SurrealDB version 1.2.2):**

- minor improvements for documentation and tests

**Version 0.2.9 (compatible with SurrealDB version 1.2.1):**

- create DatabaseConnectionsPool for use at a high load
- improve documentation and examples

**Version 0.2.8 (compatible with SurrealDB version 1.2.1):**

- database now can use DEFINE FIELD, DEFINE TABLE
- Select now can use multiple aliases (AS)
- improve documentation and examples

**Version 0.2.7 (compatible with SurrealDB version 1.2.1):**

- support a new version
- Database object now can use and kill a live query
- readable error on misspell Database object name

**Version 0.2.6 (compatible with SurrealDB version 1.2.0):**

- websockets became much faster (both for single and multithreading)
- improve documentation

**Version 0.2.5 (compatible with SurrealDB version 1.2.0):**

- improve documentation about LQ and RecordID
- add first() and last() methods for Result
- add constants(math) tests

**Version 0.2.4 (compatible with SurrealDB version 1.2.0):**

- using IF EXISTS in all tables removing
- improve documentation
- add some tests for string::semver

**Version 0.2.3 (compatible with SurrealDB version 1.1.1):**

- database now can use RELATE, RETURN, DEFINE TOKEN
- improve documentation and examples

**Version 0.2.2 (compatible with SurrealDB version 1.1.1):**

- Select query now can use iterator to traverse
  results, [see ](https://github.com/kotolex/surrealist/tree/master?tab=readme-ov-file#iteration-on-select)
- improve documentation and examples

**Version 0.2.1 (compatible with SurrealDB version 1.1.1):**

- Database now can define/remove index, analyzer and scope
- improve documentation and examples

**Version 0.2.0 (compatible with SurrealDB version 1.1.1):**

- QL-constructor added, you can now generate queries and use all features of QL
- result now has helpers like is_empty, to_dict, id, ids, get
- websocket connection works faster
- a lot more examples added
- updated readme on all updates and features
- bug fixes

**Version 0.1.8 (compatible with SurrealDB version 1.1.1):**

- grammar and spelling fixes

**Version 0.1.6 (compatible with SurrealDB version 1.1.1):**

- add changes feed ability
- add examples folder
- select now always returns a list
- create now always returns a dict (not list)

**Version 0.1.5 (compatible with SurrealDB version 1.1.1):**

- http transport now can use PATCH (via a query)
- remove INFO and add db_info, ns_info, root_info
- fix bug with http methods when using table_name "table:id"
- add docs to connection(parent for transports), including features

**Version 0.1.4 (compatible with SurrealDB version 1.1.1):**

- now you can use a custom live query, not just a simple one, look above in LQ part
- added some helper function to get database info, session info, table names
- add Release Notes to docs
- now even errors return as SurrealResult
- exceptions will be raised only if nothing we can do