import logging
from typing import Optional, Tuple, List, Dict, Union, Any

from surrealist import Surreal, SurrealResult
from surrealist.ql.statements import Select, Remove
from surrealist.ql.statements.define import (DefineEvent, DefineUser, DefineParam, DefineAnalyzer, DefineScope,
                                             DefineIndex)
from surrealist.ql.statements.statement import Statement
from surrealist.ql.statements.transaction import Transaction
from surrealist.ql.table import Table
from surrealist.utils import DEFAULT_TIMEOUT

logger = logging.getLogger("databaseQL")


class Database:
    """
    Represents connected database(in some namespace) to operate on.
    It has features of the database level, including switch to table level. You can use DEFINE/REMOVE, query or
    transactions here, but for simple CRUD or live queries you need to switch to table level

    Examples: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py
    """

    def __init__(self, url: str, namespace: str, database: str, credentials: Optional[Tuple[str, str]],
                 use_http: bool = False, timeout: int = DEFAULT_TIMEOUT, log_level: str = "ERROR"):
        self._namespace = namespace
        self._database = database
        self._connection = Surreal(url, namespace, database, credentials, use_http, timeout, log_level).connect()
        self._connected = True
        logger.info("DatabaseQL is up")

    def __enter__(self):
        return self

    def __exit__(self, *exc_details):
        self.close()

    def is_connected(self) -> bool:
        """
        Return if a database is connected
        :return: True if connection is active, False otherwise
        """
        return self._connected

    @property
    def namespace(self) -> str:
        """
        Return name if the namespace
        :return: namespace
        """
        return self._namespace

    @property
    def name(self) -> str:
        """
        Return name of the database
        :return: database name
        """
        return self._database

    def close(self):
        """
        Closes the connection. You cannot and should not use a database object after that
        """
        logger.info("DatabaseQL is down")
        self._connection.close()
        self._connected = False

    def tables(self) -> List[str]:
        """
        Return list of the table names at a current database
        :return: string list with the names
        """
        logger.info("Get tables for db %s", self._database)
        return self._connection.db_tables().result

    def info(self) -> Dict:
        """
        Return full info about a database, actually call for "INFO FOR DB;" via query
        :return: a result of the response
        """
        logger.info("Get info for db %s", self._database)
        return self._connection.db_info().result

    def raw_query(self, query: str) -> SurrealResult:
        """
        Execute a raw QL query on a database, you should use it in cases when your query is large or complicated, or it
        is just more appropriate for you.

        :param query: text of the query, it will be sent to SurrealDB as is
        :return: a result of the response
        """
        logger.info("Query for db %s", self._database)
        return self._connection.query(query)

    def __getattr__(self, item) -> Table:
        """
        We actually need this to use dot as the switch to table level as "db.person" instead of "db.table("person")"
        :param item: name of the table to work with
        :return: a table object
        """
        return Table(item, self._connection)

    def table(self, name) -> Table:
        """
        Switch to the Table level(an object) after that you can and should use table operations (CRUD)

        :param name: name of the table to work with
        :return: a table object
        """
        return Table(name, self._connection)

    def transaction(self, statements: List[Statement]) -> Transaction:
        """
        Create a transaction object to generate a query or run

        Refer to: https://docs.surrealdb.com/docs/surrealql/transactions

        :param statements: list of appropriate statements (select, create, delete. etc.)
        :return: Transaction object
        """
        return Transaction(self._connection, statements)

    def select_from(self, select: Select, *args, alias: Optional[Tuple[str, Union[str, Statement]]] = None,
                    value: Optional[str] = None) -> Select:
        """
        Allow selecting from sub-query (select)

        :param select: Select object
        :param args: which fields to select, if no fields or "*" - selects all
        :param alias: pairs of names and values
        :param value: on exists add VALUE operator
        :return: Select object
        """
        return Select(self._connection, select, *args, alias=alias, value=value)

    def define_event(self, name: str, table_name: str, then: Union[str, Statement]) -> DefineEvent:
        """
        Allow defining event on table

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/event

        Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

        :param name: name for the event
        :param table_name: name of the table
        :param then: action to perform
        :return: DefineEvent object
        """
        return DefineEvent(self._connection, name, table_name, then)

    def remove_event(self, name: str, table_name: str) -> Remove:
        """
        Remove an event linked to table

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/remove

        :param name: name of the event
        :param table_name: name of the table
        :return: Remove object
        """
        return Remove(self._connection, table_name=table_name, type_="EVENT", name=name)

    def define_user(self, user_name: str, password: str) -> DefineUser:
        """
        Allow defining user for a current database

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/user

        Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

        :param user_name: name for the new user
        :param password: password for user
        :return: DefineUser object
        """
        return DefineUser(self._connection, user_name=user_name, password=password)

    def remove_user(self, user_name: str) -> Remove:
        """
        Remove user of the database

        :param user_name: name of the user
        :return: Remove object
        """
        return Remove(self._connection, "", type_="USER", name=user_name)

    def define_param(self, name: str, value: Any) -> DefineParam:
        """
        Represents DEFINE PARAM operator

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/param

        Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

        :param name: name of the parameter
        :param value: value for the parameter
        :return: DefineParam object
        """
        return DefineParam(self._connection, name, value)

    def remove_param(self, name: str) -> Remove:
        """
        Remove parameter of the database

        :param name: name of the parameter
        :return: Remove object
        """
        return Remove(self._connection, "", type_="PARAM", name=name)

    def define_analyzer(self, name: str) -> DefineAnalyzer:
        """
        Represents DEFINE ANALYZER operator

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/analyzer

        Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

        :param name: name for the analyzer
        :return: DefineAnalyzer object
        """
        return DefineAnalyzer(self._connection, name)

    def remove_analyzer(self, name: str) -> Remove:
        """
        Remove the analyzer

        :param name: name of the analzer
        :return: Remove object
        """
        return Remove(self._connection, '', type_="ANALYZER", name=name)

    def define_scope(self, name: str, duration: str, signup: Union[str, Statement],
                     signin: Union[str, Statement]) -> DefineScope:
        """
        Represents DEFINE SCOPE operator

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/scope

        Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

        :param name: name for the new scope
        :param duration: session duration, like 24h
        :param signup: Create operator with string or Statement representation
        :param signin: Select operator with string or Statement representation
        :return: DefineScope object
        """
        return DefineScope(self._connection, name, duration, signup, signin)

    def remove_scope(self, name: str) -> Remove:
        """
        Remove the scope

        :param name: name of the scope
        :return: Remove object
        """
        return Remove(self._connection, "", type_="SCOPE", name=name)

    def define_index(self, name: str, table_name: str) -> DefineIndex:
        """
        Represents DEFINE INDEX operator

        Refer to: https://docs.surrealdb.com/docs/surrealql/statements/define/indexes

        Example: https://github.com/kotolex/surrealist/blob/master/examples/surreal_ql/database.py

        :param name: name for the index
        :param table_name: name of table to work with
        :return: DefineINdex object
        """
        return DefineIndex(self._connection, name, table_name)

    def remove_index(self, name: str, table_name: str) -> Remove:
        """
        Remove index by name on the table

        :param name: name og the index
        :param table_name: name of the table
        :return: Remove object
        """
        return Remove(self._connection, name=name, table_name=table_name, type_="INDEX")

    def __repr__(self):
        return f"Database(namespace={self._namespace}, name={self._database}, connected={self.is_connected()})"
