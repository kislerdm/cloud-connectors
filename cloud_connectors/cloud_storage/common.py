# Dmitry Kisler © 2020-present
# www.dkisler.com

from abc import ABC, abstractmethod
from typing import List


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
    def ls(self, 
           path: str = None) -> List[str]:
        """"Function to list buckets, or objects group in a bucket.
        
        Args:
          path: Path to the bucket objects group ala "folder".
          
        Returns:
          List of objects.
        """
        pass
    
    @abstractmethod
    def read(self,
             path: str) -> None:
        """"Function to read the object from a bucket into memory.
        
        Args:
          path: Path to locate the object in a bucket.
        """
        pass
    
    @abstractmethod
    def store(self,
              obj: bytes,
              path: str) -> None:
        """"Function to store the object from memory into bucket.
        
        Args:
          obj: Data to store in a bucket.
          path: Path to store the object to.
        """
        pass

    @abstractmethod
    def upload(self,
               path_source: str,
               path_destination: str) -> None:
        """"Function to upload the object from disk into a bucket.
        
        Args:
          path_source: Path to locate the object on fs.
          path_destination: Path to store the object to.
        """
        pass

    @abstractmethod
    def download(self,
                 path_source: str,
                 path_destination: str) -> None:
        """"Function to download the object from a bucket onto disk.
        
        Args:
          path_source: Path to locate the object in bucket.
          path_destination: Fs path to store the object to.
        """
        pass
    
    @abstractmethod
    def mv(self,
           path_source: str,
           path_destination: str) -> None:
        """"Function to move/rename the object.
        
        Args:
          path_source: Initial path to locate the object in bucket.
          path_destination: Final path to locate the object in bucket.
        """
        pass

    @abstractmethod
    def delete_object(self,
                      path: str) -> None:
        """"Function to delete the object from a bucket.
        
        Args:
          path: Path to locate the object in bucket.
        """
        pass

    @abstractmethod
    def delete_bucket(self,
                      name: str) -> None:
        """"Function to delete the bucket.
        
        Args:
          name: Bucket name.
        """
        pass
