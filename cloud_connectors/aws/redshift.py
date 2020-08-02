# Dmitry Kisler © 2020-present
# www.dkisler.com

from collections import namedtuple
from typing import List, Tuple, Any, NamedTuple
import psycopg2
import psycopg2.extras
import fastjsonschema
from cloud_connectors import exceptions


class Client:
    """Redshift/postgres compatible database client.

    Args:
        config: Config dict.

        Example (default values):
            {
                "host": "localhost",
                "dbname": "postgres",
                "port": 5432,
                "user": "postgres",
                "password": "postgres",
            }

        autocommit: Activate autocommit.

    Raises:
        exceptions.ConfigurationError: Raised when wrong connection configuration provided.
        exceptions.DatabaseConnectionError: Raises when db connection failed.
    """
    __slots__ = ["conn", "autocommit"]

    SCHEMA = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "description": "DB connection config.",
        "required": [
            "dbname",
            "user",
            "password",
        ],
        "additionalProperties": False,
        "properties": {
            "dbname": {
                "type": "string",
                "description": "DB name.",
                "default": "postgres",
            },
            "user": {
                "type": "string",
                "description": "Auth user.",
                "default": "postgres",
            },
            "password": {
                "type": "string",
                "description": "Auth password.",
                "default": "postgres",
            },
            "host": {
                "type": "string",
                "description": "DB host.",
                "default": "localhost",
            },
            "port": {
                "type": "integer",
                "description": "DB connection port.",
                "default": 5432,
            },
        },
    }

    CONF_DEFAULT = {k: v['default']
                    for k, v in SCHEMA['properties'].items()}

    RESULT_TUPLE = namedtuple("query_result", ["col_names", "values"])

    def __init__(self,
                 config: dict = CONF_DEFAULT,
                 autocommit: bool = True) -> None:
        try:
            _ = fastjsonschema.validate(Client.SCHEMA, config)
        except fastjsonschema.JsonSchemaException as ex:
            raise exceptions.ConfigurationError(ex)

        try:
            self.conn = psycopg2.connect(**config)
        except psycopg2.DatabaseError as ex:
            raise exceptions.DatabaseConnectionError(ex)

        self.autocommit = autocommit
        self.conn.set_session(autocommit=self.autocommit)

    def query_fetch(self,
                    query: str) -> NamedTuple("query_result",
                                              [("col_names", List[str]),
                                               ("values", List[Tuple[Any]])]):
        """Run query to fetch data.

        Args:
          query: SQL statement.

        Returns:
          Query results.

        Raises:
          exceptions.DatabaseError: Raised when DB, or data related error occurred.
        """
        def _col_names(cur) -> List[str]:
            """Function to extract column names.
            Args:
              cur: Cursor.
            Returns:
              List of column names.
            """
            return [desc[0] for desc in cur.description]

        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return Client.RESULT_TUPLE(col_names=_col_names(cur),
                                           values=cur.fetchall())
        except psycopg2.Error as ex:
            raise exceptions.DatabaseError(ex)

    def query_cud(self,
                  query: str) -> None:
        """Run query to create, update, delete, unload, load data.

        Args:
          query: SQL statement.

        Raises:
          cloud_connectors.database.exceptions.DatabaseError:
            Raised when DB, or data related error occurred.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
            if self.autocommit:
                self.conn.commit()
        except psycopg2.Error as ex:
            raise exceptions.DatabaseError(ex)

    def commit(self):
        """Method to commit transaction."""
        self.conn.commit()

    def rollback(self):
        """Method to rollback transaction."""
        self.conn.rollback()

    def close(self):
        """Method to close conenction."""
        self.conn.close()
