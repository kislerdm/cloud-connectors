# Dmitry Kisler © 2020-present
# www.dkisler.com

from typing import List, Tuple
from fastjsonschema import validate, JsonSchemaException
from google.cloud import storage
from cloud_connectors.template.cloud_storage import Client as ClientCommon
from cloud_connectors import exceptions


class Client(ClientCommon):
    """GCP Cloud Storage client.

    Args:
      configuration (dict): Connection configuration.

        Dict structure with all options
          See details:
            https://googleapis.dev/python/storage/latest/client.html?highlight=list%20buckets#google.cloud.storage.client.Client


    Raises:
      exceptions.ConfigurationError: Raised when provided connection configuration is wrong.
    """

    # fmt: off
    # pylint: disable=C0301
    CLIENT_CONFIG_SCHEMA = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "title": "GCP gcs client config.",
        "default": None,
        "additionalProperties": False,
        "properties": {
            "project": {
                "type": "string",
                "description": "The project which the client acts on behalf of."
            },
            "credentials": {
                "type": "object",
                "description": "The OAuth2 Credentials to use for this client.",
                "default": None,
                "additionalProperties": False,
                "properties": {
                    "token": {
                        "type": "string",
                        "description": "The bearer token that can be used in HTTP headers to make authenticated requests."
                    },
                    "expired": {
                        "type": "boolean",
                        "description": "Checks if the credentials are expired."
                    },
                    "valid": {
                        "type": "boolean",
                        "description": "Checks the validity of the credentials."
                    },
                    "expiry": {
                        "type": "string",
                        "format": "date-time",
                        "description": "When the token expires and is no longer valid."
                    },
                },
            },
            "client_info": {
                "type": "object",
                "description": "The client info used to send a user-agent string with requests.",
                "default": None,
                "additionalProperties": False,
                "properties": {
                    "python_version": {
                        "type": "string",
                        "description": "The Python interpreter version."
                    },
                    "grpc_version": {
                        "type": "string",
                        "description": "The gRPC library version."
                    },
                    "api_core_version": {
                        "type": "string",
                        "description": "The google-api-core library version."
                    },
                    "gapic_version": {
                        "type": "string",
                        "description": "The sversion of gapic-generated client library."
                    },
                    "client_library_version": {
                        "type": "string",
                        "description": "The version of the client library."
                    },
                    "user_agent": {
                        "type": "string",
                        "description": "Prefix to the user agent header."
                    }
                }
            },
            "client_options": {
                "type": "object",
                "description": "Client options used to set user options on the client.",
                "default": None,
                "additionalProperties": False,
                "properties": {
                    "api_endpoint": {
                        "type": "string",
                        "description": "The desired API endpoint.",
                        "examples": [
                            "compute.googleapis.com",
                        ]
                    },
                    "quota_project_id": {
                        "type": "string",
                        "description": "A project name that a client’s quota belongs to.",
                    },
                    "credentials_file": {
                        "type": "string",
                        "description": "A path to a file storing credentials.",
                    },
                    "scopes": {
                        "type": "array",
                        "description": "OAuth access token override scopes.",
                        "items": {
                            "type": "string",
                        },
                        "examples": [
                            [
                                "https://www.googleapis.com/auth/devstorage.full_control",
                                "https://www.googleapis.com/auth/devstorage.read_only"
                            ]
                        ]
                    }
                }
            }
        }
    }
    # fmt: on

    def __init__(self, configuration: dict = None):
        if configuration:
            try:
                _ = validate(Client.CLIENT_CONFIG_SCHEMA, configuration)
            except JsonSchemaException as ex:
                raise exceptions.ConfigurationError(ex)

            if "credentials" in configuration:
                if configuration['credentials']:
                    if "expiry" in configuration['credentials']:
                        configuration['credentials']['expiry'] = time.strptime(
                            configuration['credentials']['expiry']
                        )
                    configuration['credentials'] = google.auth.credentials.Credentials(
                        **configuration['credentials']
                    )

            if "client_info" in configuration:
                if configuration['client_info']:
                    configuration['client_info'] = google.api_core.client_info.ClientInfo(
                        **configuration['client_info']
                    )

        self.client = storage.Client(**configuration) if configuration else storage.Client()

    def list_buckets(self) -> List[str]:
        """Function to list buckets.

        Returns:
          List of buckets.
        """
        return [bucket.id for bucket in list(self.client.list_buckets())]

    def list_objects(self, bucket: str, prefix: str = "", max_objects: int = None) -> List[str]:
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
        bucket_obj = self.client.lookup_bucket(bucket)
        if not bucket_obj:
            raise exceptions.BucketNotFound(f"Bucket '{bucket}' not found.")

        return [i.name for i in bucket_obj.list_blobs(prefix=prefix, max_results=max_objects)]

    def list_objects_size(
        self, bucket: str, prefix: str = "", max_objects: int = None
    ) -> List[Tuple[str, int]]:
        # pylint: disable=protected-access
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
        bucket_obj = self.client.lookup_bucket(bucket)
        if not bucket_obj:
            raise exceptions.BucketNotFound(f"Bucket '{bucket}' not found.")

        return [
            (i.name, int(i._properties['size']))
            for i in bucket_obj.list_blobs(prefix=prefix, max_results=max_objects)
        ]
