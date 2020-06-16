# Dmitry Kisler © 2020-present
# www.dkisler.com

import psycopg2
import psycopg2.extras
import fastjsonschema
from collections import namedtuple
from typing import List, Tuple, Any, NamedTuple
from cloud_connectors.cloud_storage.common import Client as ClientCommon
import cloud_connectors.database.exceptions


class Client:
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
        """Postgres client
        
        Args:
          config: Config dict.
          
            Example (default values):
              {
                "host": "localhost",
                "dhname": "postgres",
                "port": 5432,
                "user": "postgres",
                "password": "postgres"
              }
          
          autocommit: Activate autocommit.
        
        Raises:
          cloud_connectors.database.exceptions.ConfigurationError: 
            Raised when wrong connection configuration provided.
          cloud_connectors.database.exceptions.DatabaseConnectionError: 
            Raises when db connection failed.
        """
        try:
            _ = fastjsonschema.validate(Client.SCHEMA, config)
        except fastjsonschema.JsonSchemaException as ex:
            raise exceptions.ConfigurationError(Exception(ex))

        try:
            config_str = " ".join([f"{k}={v}" for k, v in config.items()])
            self.conn = psycopg2.connect(config_str)
        except psycopg2.DatabaseError as ex:
            raise exceptions.DatabaseConnectionError(Exception(ex))

        self.conn.set_session(autocommit=True)

        return

    def query_fetch(self,
                    query: str,
                    commit: bool = True) -> NamedTuple("query_result",
                                                       [("col_names", List[str]),
                                                        ("values", List[Tuple[Any]])]):
        """Run query to fetch data.
        
        Args:
          query: SQL statement.
          commit: Commit 
        
        Returns:
          Query results.
        
        Raises:
          cloud_connectors.database.exceptions.DatabaseError: 
            Raised when DB, or data related error occurred.
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
            raise exceptions.DatabaseError(Exception(ex))

    def query_cud(self,
                  query: str,
                  commit: bool = True) -> None:
        """Run query to create, update, delete data.
        
        Args:
          query: SQL statement.
          commit: Commit
        
        Raises:
          cloud_connectors.database.exceptions.DatabaseError: 
            Raised when DB, or data related error occurred.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)

            if commit:
                self.conn.commit()
        except psycopg2.Error as ex:
            raise exceptions.DatabaseError(Exception(ex))
        return

    def write_batch(self,
                    table: str,
                    data: List[dict],
                    batch_size: int = 100) -> None:
        """Function to write data from memory into table.
        Inspired by: https://hakibenita.com/fast-load-data-python-postgresql
        
        Args:
          table: Table name, e.g. test, or public.test.
          data: Data dict. !Note! It must be a flat json.
          batch_size: Batch size to use for data writing.
        
        Raises:
          cloud_connectors.database.exceptions.DataStructureError: 
            Raised when provided data don't match requirements.
          cloud_connectors.database.exceptions.DatabaseError: 
            Raised when DB, or data related error occurred.
        """
        data_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "array",
            "items": [
                {
                    "type": "object",
                    "required": [],
                    "properties": {},
                },
            ],
        }

        try:
            _ = fastjsonschema.validate(data_schema, data)
        except fastjsonschema.JsonSchemaException as ex:
            raise exceptions.DataStructureError(ex)

        cols = ", ".join(data[0].keys())
        fields = ", ".join([f"%({k})s" for k in data[0].keys()])
        query = f"""INSERT INTO {table} ({cols}) VALUES ({fields});"""

        try:
            with self.conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur,
                                              query,
                                              data,
                                              page_size=batch_size)

        except psycopg2.Error as ex:
            raise exceptions.DatabaseError(Exception((f"Query: {query}\nError: {ex}"))
        return

    def commit(self) -> None:
        """Commit transactions."""
        self.conn.commit()
        return

    def rollback(self) -> None:
        """Rollback transactions."""
        self.conn.rollback()
        return

    def close(self) -> None:
        """Close connection."""
        self.conn.close()
        return
