# Dmitry Kisler © 2020-present
# www.dkisler.com

from abc import ABC, abstractmethod
from typing import List, Tuple


class Client(ABC):
    """Client abstract class to connect to a cloud storage service.
    
    Args:
      configuration: Connection configuration.
    """
    @abstractmethod
    def __init__(self, 
                 configuration: dict):
        pass

    @abstractmethod
    def list_buckets(self) -> List[str]:
        """"Function to list buckets.
          
        Returns:
          List of buckets.
        """
        pass
    
    @abstractmethod
    def list_objects(self,
                     bucket: str,
                     prefix: str = None) -> List[Tuple[str, int]]:
        """"Function to list objects in a bucket.
        
        Args:
          bucket: Bucket name.
          prefix: Objects prefix to restrict the list of results.
          
        Returns:
          List of tuples with object name and its size in bytes.
        """
        pass
    
    @abstractmethod
    def read(self,
             bucket: str,
             path: str) -> bytes:
        """"Function to read the object from a bucket into memory.
        
        Args:
          bucket: Bucket name.
          path: Path to locate the object in a bucket.
        """
        pass
    
    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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
    
    @abstractmethod
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
    
    @abstractmethod
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

    @abstractmethod
    def delete_object(self,
                      bucket: str,
                      path: str) -> None:
        """"Function to delete the object from a bucket.
        
        Args:
          bucket: Bucket name.
          path: Path to locate the object in bucket.
        """
        pass
    
    @abstractmethod
    def delete_objects(self,
                       bucket: str,
                       paths: List[str]) -> None:
        """Function to delete the objects from a bucket.

        Args:
          bucket: Bucket name.
          paths: Paths to locate the objects in bucket.
        """
        pass