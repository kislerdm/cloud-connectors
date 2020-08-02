# pylint: disable=missing-function-docstring,logging-fstring-interpolation
import sys
import logging
import warnings
from fastjsonschema import validate, JsonSchemaException
from moto import mock_sts  # type: ignore
from cloud_connectors.aws import sts as module
from cloud_connectors.aws.s3 import Client as s3_client


logging.basicConfig(level=logging.ERROR,
                    format="[line: %(lineno)s] %(message)s")
LOGGER = logging.getLogger(__name__)
warnings.simplefilter(action="ignore", category=FutureWarning)

OBJECTS = {"CONFIG_SCHEMA", "assume_role"}


def test_module_objects_missing() -> None:
    missing = OBJECTS.difference(set(module.__dir__()))
    if missing:
        LOGGER.error(
            f"""Object(s) '{"', '".join(missing)}' definition is(are) missing."""
        )
        sys.exit(1)


@mock_sts
def test_assume_role() -> None:
    OUTPUT_KEYS = {"aws_access_key_id",
                   "aws_secret_access_key", "aws_session_token"}

    tests = [
        {
            "valid": True,
            "in": {
                "region_name": "eu-central-1",
                "role_arn": "arn:aws:iam::111111111111:role/test",
            },
        },
        {
            "valid": False,
            "in": {
                "region_name": "eu-central-1",
                "role_arn": "arn:aws:iam::11111111111:role/test",
            },
        },
    ]

    for test in tests:
        if test["valid"]:
            temp_credentials = module.assume_role(test["in"])
            missing = OUTPUT_KEYS.difference(set(temp_credentials.keys()))
            if missing:
                LOGGER.error(
                    f"""Key(s) '{"', '".join(missing)}' is(are) missing - 'assume_role' method."""
                )
                sys.exit(1)

            try:
                _ = validate(s3_client.CLIENT_CONFIG_SCHEMA, temp_credentials)
            except JsonSchemaException as ex:
                LOGGER.error("Temp keys validation failure.")
                sys.exit(1)
        else:
            try:
                temp_credentials = module.assume_role(test["in"])
            except Exception as ex:
                if type(ex).__name__ != "ConfigurationError":
                    LOGGER.error("Wrong error type to handle Config error error")
                    sys.exit(1)


def test_assume_role_exception() -> None:
    test = {
        "region_name": "eu-central-1",
        "role_arn": "arn:aws:iam::111111111111:role/test",
    }

    try:
        _ = module.assume_role(test)
    except Exception as ex:
        if type(ex).__name__ != "ConnectionError":
            LOGGER.error("Faulty connection error handling.")
            sys.exit(1)
