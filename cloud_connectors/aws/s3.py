# Dmitry Kisler © 2020-present
# www.dkisler.com

import os
from typing import List, Tuple
import boto3
from fastjsonschema import validate, JsonSchemaException
from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError
from cloud_connectors.template.cloud_storage import Client as ClientCommon
from cloud_connectors import exceptions


class Client(ClientCommon):
    """AWS s3 client.

    Args:
      configuration (dict): Connection configuration.

        Dict structure with all options
          See details:
            https://github.com/boto/boto3/blob/master/boto3/session.py, method client
          See config key:
            https://botocore.amazonaws.com/v1/documentation/api/1.17.2/reference/config.html

    Raises:
      exceptions.ConnectionError: Raised when a connection error to s3 occurred.
      exceptions.ConfigurationError: Raised when provided connection configuration is wrong.
    """

    # fmt: off
    # pylint: disable=C0301
    CLIENT_CONFIG_SCHEMA = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "title": "AWS S3 client config.",
        "default": None,
        "additionalProperties": False,
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
            "api_version": {
                "type": "string",
                "description": "The API version to use.  By default, botocore will use the latest API version when creating a client."
            },
            "use_ssl": {
                "type": "boolean",
                "description": "Whether or not to use SSL.",
                "default": True
            },
            "verify": {
                "oneOf": [
                    {"type": "boolean"},
                    {"type": "string"}
                ],
                "description": "Whether or not to verify SSL certificates.",
                "examples": [
                    True,
                    False,
                    "path/to/cert/bundle.pem"
                ]
            },
            "endpoint_url": {
                "type": "string",
                "description": """The complete URL to use for the constructed client.
                Normally, botocore will automatically construct the appropriate URL to use when communicating with a service."""
            },
            "aws_access_key_id": {
                "type": "string",
                "description": "The access key to use when creating the client.",
                "pattern": "A[K|S]IA[A-Z0-9]{16}",
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
            "aws_session_token": {
                "type": "string",
                "description": "AWS token, usually used with temp secrets when assuming AWS role.",
            },
            "config": {
                "type": "object",
                "description": "Advanced client configuration options. https://botocore.amazonaws.com/v1/documentation/api/1.17.2/reference/config.html",
                "default": None,
                "additionalProperties": False,
                "properties": {
                    "region_name": {
                        "type": "string",
                        "description": "The region to use in instantiating the client."
                    },
                    "signature_version": {
                        "type": "string",
                        "description": "The signature version when signing requests."
                    },
                    "user_agent": {
                        "type": "string",
                        "description": "The value to use in the User-Agent header.",
                        "default": None
                    },
                    "user_agent_extra": {
                        "type": "string",
                        "description": "The value to append to the current User-Agent header.",
                        "default": None
                    },
                    "connect_timeout": {
                        "oneOf": [
                            {"type": "number"},
                            {"type": "integer"}
                        ],
                        "description": "The time in seconds till a timeout exception is thrown when attempting to make a connection.",
                        "default": 60
                    },
                    "read_timeout": {
                        "oneOf": [
                            {"type": "number"},
                            {"type": "integer"}
                        ],
                        "description": "The time in seconds till a timeout exception is thrown when attempting to read from a connection.",
                        "default": 60
                    },
                    "parameter_validation": {
                        "type": "boolean",
                        "description": "Whether parameter validation should occur when serializing requests.",
                        "default": True
                    },
                    "max_pool_connections": {
                        "type": "integer",
                        "description": "he maximum number of connections to keep in a connection pool.",
                        "default": 10
                    },
                    "proxies": {
                        "type": "object",
                        "description": "A dictionary of proxy servers to use by protocol or endpoint. The proxies are used on each request.",
                        "default": None,
                        "examples": [
                            {
                                "http": "foo.bar:3128",
                                "http://hostname": "foo.bar:4012"
                            }
                        ]
                    },
                    "s3": {
                        "type": "object",
                        "description": "A dictionary of s3 specific configurations.",
                        "default": {
                            "use_accelerate_endpoint": False,
                            "payload_signing_enabled": False,
                            "addressing_style": "auto"
                        },
                        "additionalProperties": False,
                        "properties": {
                            "use_accelerate_endpoint": {
                                "type": "boolean",
                                "description": """Refers to whether to use the S3 Accelerate endpoint.
                                If true, the client will use the S3 Accelerate endpoint.
                                If the S3 Accelerate endpoint is being used then the addressing style will always be virtual."""
                            },
                            "payload_signing_enabled": {
                                "type": "boolean",
                                "description": "Refers to whether or not to SHA256 sign sigv4 payloads."
                            },
                            "addressing_style": {
                                "type": "string",
                                "description": "Refers to the style in which to address s3 endpoints.",
                                "default": "auto",
                                "enum": [
                                    "auto",
                                    "virtual",
                                    "path"
                                ]
                            }
                        }
                    },
                    "retries": {
                        "type": "object",
                        "description": "A dictionary for retry specific configurations.",
                        "additionalProperties": False,
                        "properties": {
                            "total_max_attempts": {
                                "type": "integer",
                                "description": "An integer representing the maximum number of total attempts that will be made on a single request."
                            },
                            "max_attempts": {
                                "type": "integer",
                                "description": "An integer representing the maximum number of retry attempts that will be made on a single request."
                            },
                            "mode": {
                                "type": "string",
                                "description": "A string representing the type of retry mode botocore should use.",
                                "enum": ["legacy", "standard", "adaptive"]
                            }
                        }
                    },
                    "client_cert": {
                        "type": "string",
                        "description": "The path to a certificate for TLS client authentication."
                    },
                    "inject_host_prefix": {
                        "type": "boolean",
                        "description": "Whether host prefix injection should occur."
                    }
                }
            }
        }
    }

    S3_TRANSFER_SCHEMA = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "title": "AWS S3 transfer config.",
        "default": None,
        "additionalProperties": False,
        "properties": {
            "multipart_threshold": {
                "type": "integer",
                "description": "The transfer size threshold for which multipart uploads, downloads, and copies will automatically be triggered.",
                "default": 8388608
            },
            "max_concurrency": {
                "type": "integer",
                "description": "The maximum number of threads that will be making requests to perform a transfer.",
                "default": 10
            },
            "multipart_chunksize": {
                "type": "integer",
                "description": "The partition size of each part for a multipart transfer.",
                "default": 8388608
            },
            "num_download_attempts": {
                "type": "integer",
                "description": "The number of download attempts that will be retried upon errors with downloading an object in S3.",
                "default": 5
            },
            "max_io_queue": {
                "type": "integer",
                "description": "The maximum amount of read parts that can be queued in memory to be written for a download.",
                "default": 100
            },
            "io_chunksize": {
                "type": "integer",
                "description": "The max size of each chunk in the io queue.",
                "default": 262144
            },
            "use_threads": {
                "type": "boolean",
                "description": "If True, threads will be used when performing S3 transfers.",
                "default": True
            },
        },
    }
    # fmt: on

    def __init__(self, configuration: dict = None) -> None:
        if configuration:
            try:
                _ = validate(Client.CLIENT_CONFIG_SCHEMA, configuration)
            except JsonSchemaException as ex:
                raise exceptions.ConfigurationError(ex)
        else:
            configuration = {}

        self.client = boto3.client("s3", **configuration)

    def list_buckets(self) -> List[str]:
        """Function to list buckets.

        Returns:
          List of buckets.

        Raises:
          ConnectionError: Raised when connection cannot be established.
        """
        try:
            resp = self.client.list_buckets()
            if resp["Buckets"]:
                return [bucket["Name"] for bucket in resp["Buckets"]]
            return []
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "InvalidAccessKeyId":
                raise ConnectionError("Invalid access key")

    def list_objects(
        self, bucket: str, prefix: str = "", max_objects: int = None
    ) -> List[str]:
        """Function to list objects in a bucket.

        Args:
          bucket: Bucket name.
          prefix: Objects prefix to restrict the list of results.
          max_objects: Max number of keys to output.

        Returns:
          List of objects path in the bucket.

        Raises:
          exceptions.BucketNotFound: Raised when the bucket not found.
        """
        return [obj["Key"] for obj in self._list_objects(
            bucket=bucket, prefix=prefix, max_objects=max_objects
        )]

    def list_objects_size(
        self, bucket: str, prefix: str = "", max_objects: int = None
    ) -> List[Tuple[str, int]]:
        """Function to list objects in a bucket with their size.

        Args:
          bucket: Bucket name.
          prefix: Objects prefix to restrict the list of results.
          max_objects: Max number of keys to output.

        Returns:
          List of tuples with objects path and size in bytes.

        Raises:
          exceptions.BucketNotFound: Raised when the bucket not found.
        """
        return [(obj["Key"], obj["Size"]) for obj in self._list_objects(
            bucket=bucket, prefix=prefix, max_objects=max_objects
        )]

    def _list_objects(
        self, bucket: str, prefix: str = "", max_objects: int = None
    ) -> List[dict]:
        """Function to list objects in a bucket with their size.

        Args:
          bucket: Bucket name.
          prefix: Objects prefix to restrict the list of results.
          max_objects: Max number of keys to output.

        Returns:
          List of objects attributes in the bucket.

        Raises:
          exceptions.BucketNotFound: Raised when the bucket not found.
        """
        output = []
        max_objects = max_objects if max_objects else 1000

        paginator = self.client.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if "Contents" in page:
                    output.extend(page["Contents"])
        except ParamValidationError as ex:
            raise exceptions.BucketNotFound(ex)
        except ClientError as ex:
            if type(ex).__name__ == "NoSuchBucket":
                raise exceptions.BucketNotFound(f"Bucket '{bucket}' not found.")

        return output

    def read(self, bucket: str, path: str) -> bytes:
        """Function to read the object from a bucket into memory.

        Args:
          bucket: Bucket name.
          path: Path to locate the object in a bucket.

        Returns:
          Bytes encoded object.

        Raises:
          ConnectionError: Raised when connection error occured.
          exceptions.ObjectNotFound: Raised when the object not found.
          exceptions.BucketNotFound: Raised when the bucket not found.
        """
        try:
            obj = self.client.get_object(Bucket=bucket, Key=path)
            return obj["Body"].read()
        except ParamValidationError as ex:
            raise exceptions.BucketNotFound(ex)
        except NoCredentialsError: # pragma: no cover
            raise ConnectionError("Cannot connect, no credentials provided")
        except Exception as ex:
            if type(ex).__name__ == "NoSuchKey":
                raise exceptions.ObjectNotFound(
                    f"Object '{path}' not found in bucket '{bucket}'"
                )
            if type(ex).__name__ in ["NoSuchBucket", "InvalidBucketName"]:
                raise exceptions.BucketNotFound(f"Bucket '{bucket}' not found: {ex}")
            raise Exception(ex) # pragma: no cover

    def write(
        self, obj: bytes, bucket: str, path: str, configuration: dict = None
    ) -> None:
        """Function to write the object from memory into bucket.

        Args:
          obj: Object data to store in a bucket.
          bucket: Bucket name.
          path: Path to store the object to.
          configuration: Extra configurations.
            See: https://boto3.amazonaws.com/v1/documentation/api/1.14.3/reference/services/s3.html?highlight=s3%20client#S3.Client.put_object
            For example:
            *.json.gz file should be uploaded with the following configuration:
                {
                    "ContentEncoding": "gzip",
                    "ContentType": "application/json"
                }

        Raises:
          ConnectionError: Raised when connection error occured.
          TypeError: Raised when provided attributes have wrong types.
          exceptions.BucketNotFound: Raised when the bucket not found.
        """
        configuration = configuration if configuration else {}
        try:
            self.client.put_object(Body=obj, Bucket=bucket, Key=path, **configuration)
        except NoCredentialsError: # pragma: no cover
            raise ConnectionError("Cannot connect, no credentials provided")
        except Exception as ex:
            if type(ex).__name__ == "NoSuchBucket":
                raise exceptions.BucketNotFound(f"Bucket '{bucket}' not found.")
            if type(ex).__name__ == "ParamValidationError":
                raise TypeError("Provided function attributes have wrong type.")
            raise Exception(ex) # pragma: no cover

    def upload(
        self, bucket: str, path_source: str, path_destination: str = None
    ) -> None:
        """Function to upload the object from disk into a bucket.

        Args:
          bucket: Bucket name.
          path_source: Path to locate the object on fs.
          path_destination: Path to store the object to.

        Raises:
          FileNotFoundError: Raised when file path_source not found.
          exceptions.BucketNotFound: Raised when the bucket not found.
        """
        if not os.path.exists(path_source):
            raise FileNotFoundError(f"{path_source} not found")

        try:
            self.client.upload_file(
                Filename=path_source,
                Bucket=bucket,
                Key=path_destination if path_destination else path_source,
            )
        except Exception as ex:
            if type(ex).__name__ == "S3UploadFailedError":
                raise exceptions.BucketNotFound(f"Bucket '{bucket}' not found.")
            raise Exception(ex) # pragma: no cover

    def download(
        self,
        bucket: str,
        path_source: str,
        path_destination: str,
        configuration: dict = None,
    ) -> None:
        """Function to download the object from a bucket to disk.

        Args:
          bucket: Bucket name.
          path_source: Path to locate the object in bucket.
          path_destination: Fs path to store the object to.
          configuration: Transfer config parameters.
            See: https://boto3.amazonaws.com/v1/documentation/api/1.14.2/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig

        Raises:
          exceptions.ObjectNotFound: Raised when the object not found.
          exceptions.BucketNotFound: Raised when the bucket not found.
          exceptions.DestinationPathError: Raised when cannot save object to provided location.
          exceptions.DestinationPathPermissionsError: Raised when cannot save object to provided
            location due to lack of permissons.
        """
        if configuration:
            try:
                _ = validate(Client.S3_TRANSFER_SCHEMA, configuration)
            except JsonSchemaException as ex:
                raise exceptions.ConfigurationError(ex)
        else:
            configuration = {k: v["default"]
                for k, v in Client.S3_TRANSFER_SCHEMA["properties"].items()
            }

        try:
            self.client.download_file(
                Filename=path_destination,
                Bucket=bucket,
                Key=path_source,
                Config=boto3.s3.transfer.TransferConfig(**configuration),
            )
        except (NotADirectoryError, FileNotFoundError):
            raise exceptions.DestinationPathError(
                f"Cannot download file to {path_destination}"
            )
        except PermissionError:
            raise exceptions.DestinationPathPermissionsError(
                f"Cannot download file to {path_destination}"
            )
        except ClientError as ex:
            if type(ex).__name__ == "NoSuchBucket":
                raise exceptions.BucketNotFound(f"Bucket '{bucket}' not found.")
            if ex.response["Error"]["Code"] == "404":
                raise exceptions.ObjectNotFound(
                    f"Object '{path_source}' not found in bucket '{bucket}'"
                )
            raise Exception(ex) # pragma: no cover

    def copy(
        self,
        bucket_source: str,
        bucket_destination: str,
        path_source: str,
        path_destination: str = None,
        configuration: dict = None,
    ) -> None:
        """Function to copy the object from bucket to bucket.

        Args:
          bucket_source: Bucket name source.
          bucket_destination: Bucket name destination.
          path_source: Initial path to locate the object in bucket.
          path_destination: Final path to locate the object in bucket.
          configuration: Extra configurations.
            #S3.Client.copy_object
            See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html?highlight=copy_object

        Raises:
          ConnectionError: Raised when a connection error to s3 occurred.
          exceptions.ObjectNotFound: Raised when the object not found.
          exceptions.BucketNotFound: Raised when the bucket not found.
        """
        configuration = configuration if configuration else {}
        if (bucket_destination, path_destination) == (bucket_source, path_source):
            configuration["MetadataDirective"] = "REPLACE"

        try:
            obj = self.client.get_object(Bucket=bucket_source, Key=path_source)
        except NoCredentialsError: # pragma: no cover
            raise ConnectionError("Cannot connect, no credentials provided")
        except ClientError as ex:
            if type(ex).__name__ == "NoSuchKey":
                raise exceptions.ObjectNotFound(
                    f"Object '{path_source}' not found in bucket '{bucket_source}'"
                )
            if type(ex).__name__ == "NoSuchBucket":
                raise exceptions.BucketNotFound(f"Bucket '{bucket_source}' not found.")
            raise Exception(ex) # pragma: no cover

        configuration["ContentType"] = obj["ContentType"]

        try:
            self.client.copy_object(
                Bucket=bucket_destination,
                CopySource={"Bucket": bucket_source, "Key": path_source,},
                Key=path_destination if path_destination else path_source,
                **configuration,
            )
        except ClientError as ex:
            if type(ex).__name__ == "NoSuchBucket":
                raise exceptions.BucketNotFound(
                    f"Bucket '{bucket_destination}' not found."
                )
            raise Exception(ex) # pragma: no cover

    def move(
        self,
        bucket_source: str,
        bucket_destination: str,
        path_source: str,
        path_destination: str = None,
        configuration: dict = None,
    ) -> None:
        """Function to move/rename the object.

        Args:
          bucket_source: Bucket name source.
          bucket_destination: Bucket name destination.
          path_source: Initial path to locate the object in bucket.
          path_destination: Final path to locate the object in bucket.
          configuration: Extra configurations.
            See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html?highlight=copy_object#S3.Client.copy_object

        Raises:
          ConnectionError: Raised when a connection error to s3 occurred.
          exceptions.ObjectNotFound: Raised when the object not found.
          exceptions.BucketNotFound: Raised when the bucket not found.
        """
        self.copy(
            bucket_source=bucket_source,
            bucket_destination=bucket_destination,
            path_source=path_source,
            path_destination=path_destination,
            configuration=configuration if configuration else {},
        )

        self.delete_object(bucket=bucket_source, path=path_source)

    def delete_object(self, bucket: str, path: str) -> None:
        """Function to delete the object from a bucket.

        Args:
          bucket: Bucket name.
          path: Path to locate the object in bucket.

        Raises:
          ConnectionError: Raised when a connection error to s3 occurred.
          exceptions.ObjectNotFound: Raised when the object not found.
          exceptions.BucketNotFound: Raised when the bucket not found.
        """
        try:
            self.client.delete_object(Bucket=bucket, Key=path)
        except Exception as ex:
            if type(ex).__name__ == "NoSuchBucket":
                raise exceptions.BucketNotFound(f"Bucket '{bucket}' not found.")
            raise Exception(ex) # pragma: no cover

    def delete_objects(self, bucket: str, paths: List[str]) -> None:
        """Function to delete the objects from a bucket.

        Args:
          bucket: Bucket name.
          paths: Paths to locate the objects in bucket.

        Raises:
          ConnectionError: Raised when a connection error to s3 occurred.
          cloud_connectors.cloud_storage.exceptions.ObjectNotFound:
            Raised when the object not found.
          cloud_connectors.cloud_storage.exceptions.BucketNotFound:
            Raised when the object not found.
        """
        try:
            self.client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": v} for v in paths], "Quiet": True,},
            )
        except Exception as ex:
            if type(ex).__name__ == "NoSuchBucket":
                raise exceptions.BucketNotFound(f"Bucket '{bucket}' not found.")
            raise Exception(ex) # pragma: no cover
