# Dmitry Kisler © 2020-present
# www.dkisler.com

import os
from typing import Union, List, Tuple
from google.cloud import storage
# from cloud_connectors.cloud_storage.common import Client as ClientCommon
# import cloud_connectors.cloud_storage.exceptions
import fastjsonschema
from common import Client as ClientCommon
import exceptions


class Client(ClientCommon):
    """GCP Cloud Storage client.

    Args:
      configuration (dict): Connection configuration.

        Dict structure with all options 
          See details: 
            https://googleapis.dev/python/storage/latest/client.html?highlight=list%20buckets#google.cloud.storage.client.Client 
          

    Raises:
      ConnectionError: Raised when a connection error to s3 occurred.
      cloud_connectors.cloud_storage.exceptions.ConfigurationError: Raised when provided connection configuration is wrong.
    """
    def __init__(self, configuration: dict = None):
        if configuration:
            err = Client._validator(Client.CLIENT_CONFIG_SCHEMA,
                                    configuration)
            if err:
                raise exceptions.ConfigurationError(Exception(err))
        try:
            self.client = storage.Client(configuration) if configuration\
                else storage.Client()
        except Exception as ex:
            raise ConnectionError(ex)
        
    def list_buckets(self) -> List[str]:
        """"Function to list buckets.

        Returns:
          List of buckets.

        Raises:
          PermissionError: Raised when a ListBuckets operation is not permitted.
          ConnectionError: Raised when a connection error to s3 occurred.
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
          PermissionError: Raised when a ListBuckets operation is not permitted.
          cloud_connectors.cloud_storage.exceptions.BucketNotFound: 
            Raised when the object not found.
        """
        raise NotImplementedError()
    
    
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
