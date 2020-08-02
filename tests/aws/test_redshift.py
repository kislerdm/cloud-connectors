# Dmitry Kisler © 2020-present
# www.dkisler.com
# pylint: disable=missing-function-docstring
import os
import sys
import inspect
import warnings
import logging
from cloud_connectors.aws import redshift as module


logging.basicConfig(level=logging.ERROR, format="[line: %(lineno)s] %(message)s")
LOGGER = logging.getLogger(__name__)
warnings.simplefilter(action="ignore", category=FutureWarning)

CLASSES = {"Client"}

CLASS_METHODS = {
    "SCHEMA",
    "CONF_DEFAULT",
    "RESULT_TUPLE",
    "query_fetch",
    "query_cud",
    "commit",
    "rollback",
    "close",
}


def test_module_miss_classes() -> None:
    missing = CLASSES.difference(set(module.__dir__()))
    if missing:
        LOGGER.error(f"""Class(es) '{"', '".join(missing)}' is(are) missing.""")
        sys.exit(1)


def test_class_client_miss_methods() -> None:
    model_members = inspect.getmembers(module.Client)
    missing = CLASS_METHODS.difference({i[0] for i in model_members})
    if missing:
        LOGGER.error(f"""Class 'Client' Method(s) '{"', '".join(missing)}' is(are) missing.""")
        sys.exit(1)


def test_validator() -> None:
    tests = [
        {
            "type": "faulty_validation",
            "config": {
                "host": "localhost",
                "dbname": "postgres",
                "port": 5432,
                "password": "postgres",
            },
        },
        {
            "type": "faulty_connection",
            "config": {
                "host": "localhost",
                "dbname": "postgres",
                "port": 1,
                "user": "postgres",
                "password": "postgres",
            },
        },
    ]

    for test in tests:
        try:
            _ = module.Client(test["config"])
        except Exception as ex:
            if test["type"] == "faulty_validation":
                if type(ex).__name__ != "ConfigurationError":
                    LOGGER.error("Wrong error type to handle config error")
                    sys.exit(1)

                if "['dbname', 'user', 'password']" not in str(ex):
                    LOGGER.error(f"Configuration validator error - user: {ex}")
                    sys.exit(1)

            elif test["type"] == "faulty_connection":
                if type(ex).__name__ != "DatabaseConnectionError":
                    LOGGER.error("Wrong error type to handle connection error")
                    sys.exit(1)


# db instance required
config = {
    "host": "localhost",
    "dbname": "postgres",
    "port": int(os.getenv("DB_PORT_TEST", "11111")),
    "user": "postgres",
    "password": "postgres",
}

skip = False

try:
    client = module.Client(config)
except Exception as ex:
    if type(ex).__name__ == "DatabaseConnectionError":
        skip = True


def test_select() -> None:
    if not skip:
        client = module.Client(config, autocommit=False)
        result = client.query_fetch("SELECT 100 AS a;")
        client.commit()
        if (result.col_names, result.values) != (["a"], [(100,)]):
            LOGGER.error("Faulty select query runner")
            sys.exit(1)

        try:
            _ = client.query_fetch("SELECT * FROM foo_bar;")
        except Exception as ex:
            if type(ex).__name__ != "DatabaseError":
                LOGGER.error("Wrong error type to handle database error")
                sys.exit(1)
        client.close()

def test_create() -> None:
    if not skip:
        client = module.Client(config)
        client.query_cud("CREATE TABLE IF NOT EXISTS test (a int);")
        client.query_cud("INSERT INTO test VALUES (100);")
        result = client.query_fetch("SELECT * FROM test;")
        if (result.col_names, result.values) != (["a"], [(100,)]):
            LOGGER.error("Faulty select query runner")
            sys.exit(1)

        client.query_cud("DROP TABLE test;")
        try:
            client.query_cud("DROP TABLE test;")
        except Exception as ex:
            if type(ex).__name__ != "DatabaseError":
                LOGGER.error("Wrong error type to handle database error")
                sys.exit(1)

        client = module.Client(config, autocommit=False)
        client.query_cud("CREATE TABLE IF NOT EXISTS test (a int);")
        client.query_cud("INSERT INTO test VALUES (100);")
        client.rollback()
        try:
            result = client.query_fetch("SELECT * FROM test;")
        except Exception as ex:
            if type(ex).__name__ != "DatabaseError":
                LOGGER.error("Rollback error")
                sys.exit(1)