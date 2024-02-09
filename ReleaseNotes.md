## Release Notes ##

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