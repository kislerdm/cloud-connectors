# Dmitry Kisler © 2020-present
# www.dkisler.com

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
      ConnectionError: Raised when a connection error to s3 occurred.
      cloud_connectors.cloud_storage.exceptions.ConfigurationError:
        Raised when provided connection configuration is wrong.
    """
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

    def __init__(self, configuration: dict = None):
        if configuration:
            err = Client._validator(Client.CLIENT_CONFIG_SCHEMA,
                                    configuration)
            if err:
                raise exceptions.ConfigurationError(Exception(err))

            if "credentials" in configuration:
                if configuration['credentials']:
                    if "expiry" in configuration['credentials']:
                        configuration['credentials']['expiry'] = time.strptime(
                            configuration['credentials']['expiry'])
                    configuration['credentials'] = google.auth.credentials.Credentials(
                        **configuration['credentials'])

            if "client_info" in configuration:
                if configuration['client_info']:
                    configuration['client_info'] = google.api_core.client_info.ClientInfo(
                        **configuration['client_info'])

        try:
            self.client = storage.Client(**configuration) if configuration\
                else storage.Client()
        except Exception as ex:
            raise ConnectionError(ex)

    def list_buckets(self) -> List[str]:
        """"Function to list buckets.

        Returns:
          List of buckets.

        Raises:
          PermissionError: Raised when a ListBuckets operation is not permitted.
          ConnectionError: Raised when a connection error to gcs occurred.
        """
        try:
            return [bucket.id for bucket in list(self.client.list_buckets())]
        except Exception as ex:
            raise PermissionError(ex)

    def list_objects(self,
                     bucket: str,
                     prefix: str = '',
                     max_objects: int = None) -> List[Tuple[str, int]]:
        """"Function to list objects in a bucket with their size.

        Args:
          bucket: Bucket name.
          prefix: Objects prefix to restrict the list of results.
          max_objects: Max number of keys to output.

        Returns:
          List of tuples with object name and its size in bytes.

        Raises:
          PermissionError: Raised when a storage.buckets.list operation is not permitted.
          cloud_connectors.cloud_storage.exceptions.BucketNotFound:
            Raised when the object not found.
        """
        try:
            bucket_obj = self.client.lookup_bucket(bucket)
            if not bucket_obj:
                raise exceptions.BucketNotFound(
                    Exception(f"Bucket '{bucket}' not found."))

            return [(i.name, int(i._properties['size']))
                    for i in bucket_obj.list_blobs(prefix=prefix,
                                                   max_results=max_objects)]

        except Exception as ex:
            raise PermissionError(ex)

    def read(self,
             bucket: str,
             path: str) -> bytes:
        """"Function to read the object from a bucket into memory.

        Args:
          bucket: Bucket name.
          path: Path to locate the object in a bucket.
        """
        pass


    def store(self,
              obj: bytes,
              bucket: str,
              path: str,
              configuration: dict = {}) -> None:
        """"Function to store the object from memory into bucket.

        Args:
          obj: Data to store in a bucket.
          path: Path to store the object to.
          bucket: Bucket name.
          configuration: Extra configurations.
        """
        pass


    def upload(self,
               bucket: str,
               path_source: str,
               path_destination: str = None,
               configuration: dict = None) -> None:
        """"Function to upload the object from disk into a bucket.

        Args:
          bucket: Bucket name.
          path_source: Path to locate the object on fs.
          path_destination: Path to store the object to.
          configuration: Transfer config parameters.
        """
        pass


    def download(self,
                 bucket: str,
                 path_source: str,
                 path_destination: str,
                 configuration: dict = None) -> None:
        """"Function to download the object from a bucket onto disk.

        Args:
          bucket: Bucket name.
          path_source: Path to locate the object in bucket.
          path_destination: Fs path to store the object to.
          configuration: Transfer config parameters.
        """
        pass


    def copy(self,
             bucket_source: str,
             bucket_destination: str,
             path_source: str,
             path_destination: str) -> None:
        """"Function to copy the object from bucket to bucket.

        Args:
          bucket_source: Bucket name source.
          bucket_destination: Bucket name destination.
          path_source: Initial path to locate the object in bucket.
          path_destination: Final path to locate the object in bucket.
        """
        pass


    def move(self,
             bucket_source: str,
             bucket_destination: str,
             path_source: str,
             path_destination: str) -> None:
        """"Function to move/rename the object.

        Args:
          bucket_source: Bucket name source.
          bucket_destination: Bucket name destination.
          path_source: Initial path to locate the object in bucket.
          path_destination: Final path to locate the object in bucket.
        """
        pass


    def delete_object(self,
                      bucket: str,
                      path: str) -> None:
        """"Function to delete the object from a bucket.

        Args:
          bucket: Bucket name.
          path: Path to locate the object in bucket.
        """
        pass


    def delete_objects(self,
                       bucket: str,
                       paths: List[str]) -> None:
        """Function to delete the objects from a bucket.

        Args:
          bucket: Bucket name.
          paths: Paths to locate the objects in bucket.
        """
        pass
