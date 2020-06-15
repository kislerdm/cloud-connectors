# Dmitry Kisler © 2020-present
# www.dkisler.com

import os
import boto3
import botocore.exceptions
from typing import Union, List, Tuple
from .common import Client as ClientCommon
import fastjsonschema


class Client(ClientCommon):
    """AWS s3 client.

    Args:
      configuration (dict): Connection configuration.

        Dict structure with all options 
            see details https://github.com/boto/boto3/blob/master/boto3/session.py, method client,
            see config key: https://botocore.amazonaws.com/v1/documentation/api/1.17.2/reference/config.html

    Raises:
      ConnectionError: Raised when a connection to s3 occurred.
      ValueError: Raised when provided connection configuration is wrong.
    """
    __slots__ = ["client"]

    CLIENT_CONFIG_SCHEMA = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "title": "AWS S3 client config.",
        "default": None,
        "additionalProperties": False,
        "properties": {
            "region_name": {
                "type": "string",
                "description": "The name of the region associated with the client."
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
                "description": "The access key to use when creating the client."
            },
            "aws_secret_access_key": {
                "type": "string",
                "description": "The secret key to use when creating the client."
            },
            "aws_session_token": {
                "type": "string",
                "description": "The session token to use when creating the client."
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
                        "description": "The value to append to the current User-Agent header value.",
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
            }
        }
    }

    @staticmethod
    def _validator(schema: dict, obj: dict) -> Union[None, str]:
        try:
            _ = fastjsonschema.validate(schema, obj)
        except fastjsonschema.JsonSchemaException as ex:
            return ex
        return

    def __init__(self, configuration: dict = {}):
        if configuration:
            err = Client._validator(Client.CLIENT_CONFIG_SCHEMA, 
                                    configuration)
            if err:
                raise ValueError(err)
        try:
            self.client = boto3.client('s3', **configuration)
        except Exception as ex:
            raise ConnectionError(ex)
    
    def ls_buckets(self) -> List[str]:
        """"Function to list buckets.
          
        Returns:
          List of buckets.
        
        Raises:
          PermissionError: Raised when a ListBuckets operation is not permitted.
          ConnectionError: Raised when a connection to s3 occurred.
        """
        try:
            resp = self.client.list_buckets()
        except Exception as ex:
            raise PermissionError(ex)
        
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ConnectionError(f"HTTP code: {resp['HTTPStatusCode']}")
        
        if resp['Buckets']:
            return [bucket['Name'] for bucket in resp['Buckets']]
        else:
            return []
        
    def ls_objects(self,
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
          PermissionError: Raised when a ListBuckets operation is not permitted.
        """
        output = []
        max_objects = max_objects if max_objects else 1000
        
        try:
            paginator = self.client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, 
                                       Prefix=prefix)
        except Exception as ex:
            raise PermissionError(ex)

        for page in pages:
            if 'Contents' in page:
                output.extend([(obj['Key'], obj['Size']) 
                              for obj in page['Contents']])
            
        return output

    def read(self,
             bucket: str,
             path: str) -> bytes:
        """"Function to read the object from a bucket into memory.
        
        Args:
          bucket: Bucket name.
          path: Path to locate the object in a bucket.
        
        Returns:
          Bytes encoded object.
          
        Raises:
          PermissionError: Raised when a s3:getObject, or read operation is not permitted.
        """
        try:
            obj = self.client.get_object(Bucket=bucket, 
                                         Key=path)
            return obj['Body'].read()
        except Exception as ex:
            raise PermissionError(ex)
        return

    def store(self,
              obj: bytes,
              bucket: str,
              path: str) -> None:
        """Function to store the object from memory into bucket.
        
        Args:
          obj: Object data to store in a bucket.
          bucket: Bucket name.
          path: Path to store the object to.
        
        Raises:
          PermissionError: Raised when a s3:putObject, or write operation is not permitted.
        """
        try:
            resp = self.client.put_object(Body=obj,
                                          Bucket=bucket,
                                          Key=path)
        except Exception as ex:
            raise PermissionError(ex)
        
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ConnectionError(f"HTTP code: {resp['HTTPStatusCode']}")
        
        return
    
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
          configuration: Transfer config parameters
            see: https://boto3.amazonaws.com/v1/documentation/api/1.14.2/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig
          
        Raises:
          FileNotFoundError: Raised when file path_source not found.
          PermissionError: Raised when a s3:putObject, or write operation is not permitted.
          ValueError: Raised when provided s3 transfer configuration is wrong.
        """
        if not os.path.exists(path_source):
            raise FileNotFoundError(f"{path_source} not found")
        
        if configuration:
            err = Client._validator(Client.S3_TRANSFER_SCHEMA,
                                    configuration)
            if err:
                raise ValueError(err)
        
        try:
            self.client.upload_file(Filename=path_source,
                                    Bucket=bucket,
                                    Key=path_destination if path_destination else path_source,
                                    Config=configuration)
        except Exception as ex:
            raise PermissionError(ex)
        return
    
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
          configuration: Transfer config parameters
            see: https://boto3.amazonaws.com/v1/documentation/api/1.14.2/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig
        
        Raises:
          PermissionError: Raised when a s3:putObject, or write operation is not permitted.
          ValueError: Raised when provided s3 transfer configuration is wrong.
        """
        if configuration:
            err = Client._validator(Client.S3_TRANSFER_SCHEMA,
                                    configuration)
            if err:
                raise ValueError(err)
        
        try:
            self.client.download_file(Filename=path_destination,
                                      Bucket=bucket,
                                      Key=path_source,
                                      Config=configuration)
        except Exception as ex:
            raise PermissionError(ex)
        
        return
    
    def mv(self,
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
        raise NotImplementedError()
        
    def delete_object(self,
                      bucket: str,
                      path: str) -> None:
        """"Function to delete the object from a bucket.
        
        Args:
          bucket: Bucket name.
          path: Path(s) to locate the object(s) in bucket.
        
        Raises:
          PermissionError: Raised when
            s3:deleteObject, s3:DeleteObjectVersion and s3:PutLifeCycleConfigurationis 
            not permitted.
          ConnectionError: Raised when a connection to s3 occurred.
        """
        try:
            resp = self.client.delete_objects(Bucket=bucket,
                                              Key=path)
        except Exception as ex:
            raise PermissionError(ex)
        
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ConnectionError(f"HTTP code: {resp['HTTPStatusCode']}")

        return
    
    def delete_objects(self,
                       bucket: str,
                       paths: List[str]) -> None:
        """"Function to delete the object from a bucket.
        
        Args:
          bucket: Bucket name.
          paths: Paths to locate the objects in bucket.
        
        Raises:
          PermissionError: Raised when
            s3:deleteObject, s3:DeleteObjectVersion and s3:PutLifeCycleConfigurationis 
            not permitted.
          ConnectionError: Raised when a connection to s3 occurred.
        """
        try:
            resp = self.client.delete_objects(Bucket=bucket,
                                              Delete={
                                                      'Objects': [{"Key": v} for v in paths],
                                                      'Quiet': True,
                                              })
        except Exception as ex:
            raise PermissionError(ex)
        
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ConnectionError(f"HTTP code: {resp['HTTPStatusCode']}")

        if "Errors" in resp:
            raise Exception(resp['Errors'])
        
        return
