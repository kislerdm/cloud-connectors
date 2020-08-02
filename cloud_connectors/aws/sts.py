from fastjsonschema import validate, JsonSchemaException
import boto3  # type: ignore
from botocore.exceptions import (PartialCredentialsError,  # type: ignore
                                 CredentialRetrievalError,  # type: ignore
                                 NoCredentialsError,  # type: ignore
                                 ClientError)  # type: ignore
from cloud_connectors.exceptions import ConfigurationError


# fmt: off
CONFIG_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "title": "AWS STS client config.",
    "default": None,
    "additionalProperties": False,
    "required": [
        "role_arn",
    ],
    "properties": {
        "region_name": {
            "type": "string",
            "description": "The name of the region associated with the client.",
            "default": "eu-central-1",
            "enum": [
                "us-east-1",
                "us-east-2",
                "us-west-1",
                "us-west-2",
                "ca-central-1",
                "eu-central-1",
                "eu-west-1",
                "eu-west-2",
                "eu-west-3",
                "eu-south-1",
                "eu-north-1",
                "me-south-1",
                "sa-east-1",
                "cn-north-1",
                "cn-northwest-1",
                "af-south-1",
                "ap-east-1",
                "ap-south-1",
                "ap-northeast-1",
                "ap-northeast-2",
                "ap-northeast-3",
                "ap-southeast-1",
                "ap-southeast-2",
            ],
        },
        "aws_access_key_id": {
            "type": "string",
            "description": "The access key to use when creating the client.",
            "pattern": "AKIA[A-Z0-9]{16}",
            "minLength": 20,
            "maxLength": 20,
        },
        "aws_secret_access_key": {
            "type": "string",
            "description": "The secret key to use when creating the client.",
            "pattern": r"[a-zA-Z0-9\+\-\=\*/\\\!\?\%\$\#\&\(\)\[\]]{40}",
            "minLength": 40,
            "maxLength": 40,
        },
        "role_arn": {
            "type": "string",
            "description": "AWS role ID/ARN to assume.",
            "pattern": r"^arn:aws:iam::[0-9]{12}:role/*.?",
        },
    },
}
# fmt: on


def assume_role(configuration: dict) -> dict:
    """Function to assume an AWS role.

    Args:
      configuration: Config dict:
        {
            "aws_access_key_id": str,
            "aws_secret_access_key": str,
            "region_name": str,
            "role_arn": str,
        }

    Returns:
      Dict with the temp credentials:
        {
            "aws_access_key_id": str,
            "aws_secret_access_key": str,
            "aws_session_token": str,
        }

    Raises:
      ConnectionError: Raised when connection cannot be established,
        e.g. credentials not found.
    """
    try:
        _ = validate(CONFIG_SCHEMA, configuration)
    except JsonSchemaException as ex:
        raise ConfigurationError(ex)

    role_arn = configuration.pop("role_arn")

    try:
        client = boto3.client("sts", **configuration)
        resp = client.assume_role(
            RoleArn=role_arn, RoleSessionName="s3-interface"
        )
    except ClientError as ex:
        if ex.response['Error']['Code'] == "InvalidClientTokenId":
            raise ConnectionError("Invalid client token")
    except (PartialCredentialsError,
        CredentialRetrievalError,
        NoCredentialsError
    ) as ex: # pragma: no cover
        raise ConnectionError(ex)

    if resp["ResponseMetadata"]["HTTPStatusCode"] == 200:
        return {
            "aws_access_key_id": resp["Credentials"]["AccessKeyId"],
            "aws_secret_access_key": resp["Credentials"]["SecretAccessKey"],
            "aws_session_token": resp["Credentials"]["SessionToken"],
        }
    return {} # pragma: no cover
