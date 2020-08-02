# Dmitry Kisler © 2020-present
# www.dkisler.com

from abc import ABC, abstractmethod
from typing import List, Tuple


class Client(ABC):
    """Client abstract class to connect to a cloud storage service.

    Args:
      configuration: Connection configuration.
    """

    __slots__ = ["client"]

    @abstractmethod
    def __init__(self, configuration: dict) -> None:
        self.client = "client"

    @abstractmethod
    def list_buckets(self) -> List[str]:
        """"Function to list buckets.

        Returns:
          List of buckets.
        """

    @abstractmethod
    def list_objects(self, bucket: str, prefix: str = None) -> List[str]:
        """"Function to list objects in a bucket.

        Args:
          bucket: Bucket name.
          prefix: Objects prefix to restrict the list of results.

        Returns:
          List of objects in the bucket.
        """

    @abstractmethod
    def list_objects_size(self, bucket: str, prefix: str = None) -> List[Tuple[str, int]]:
        """Function to list objects in a bucket with their size.

        Args:
          bucket: Bucket name.
          prefix: Objects prefix to restrict the list of results.

        Returns:
          List of tuples with objects path and size in bytes.
        """

    @abstractmethod
    def read(self, bucket: str, path: str) -> bytes:
        """"Function to read the object from a bucket into memory.

        Args:
          bucket: Bucket name.
          path: Path to locate the object in a bucket.
        """

    @abstractmethod
    def write(
        self, obj: bytes, bucket: str, path: str, configuration: dict = {}
    ) -> None:
        """"Function to store the object from memory into bucket.

        Args:
          obj: Data to store in a bucket.
          path: Path to store the object to.
          bucket: Bucket name.
          configuration: Extra configurations.
        """

    @abstractmethod
    def upload(
        self,
        bucket: str,
        path_source: str,
        path_destination: str = None,
        configuration: dict = None,
    ) -> None:
        """"Function to upload the object from disk into a bucket.

        Args:
          bucket: Bucket name.
          path_source: Path to locate the object on fs.
          path_destination: Path to store the object to.
          configuration: Transfer config parameters.
        """

    @abstractmethod
    def download(
        self,
        bucket: str,
        path_source: str,
        path_destination: str,
        configuration: dict = None,
    ) -> None:
        """"Function to download the object from a bucket onto disk.

        Args:
          bucket: Bucket name.
          path_source: Path to locate the object in bucket.
          path_destination: Fs path to store the object to.
          configuration: Transfer config parameters.
        """

    @abstractmethod
    def copy(
        self,
        bucket_source: str,
        bucket_destination: str,
        path_source: str,
        path_destination: str,
    ) -> None:
        """"Function to copy the object from bucket to bucket.

        Args:
          bucket_source: Bucket name source.
          bucket_destination: Bucket name destination.
          path_source: Initial path to locate the object in bucket.
          path_destination: Final path to locate the object in bucket.
        """

    @abstractmethod
    def move(
        self,
        bucket_source: str,
        bucket_destination: str,
        path_source: str,
        path_destination: str,
    ) -> None:
        """"Function to move/rename the object.

        Args:
          bucket_source: Bucket name source.
          bucket_destination: Bucket name destination.
          path_source: Initial path to locate the object in bucket.
          path_destination: Final path to locate the object in bucket.
        """

    @abstractmethod
    def delete_object(self, bucket: str, path: str) -> None:
        """"Function to delete the object from a bucket.

        Args:
          bucket: Bucket name.
          path: Path to locate the object in bucket.
        """

    @abstractmethod
    def delete_objects(self, bucket: str, paths: List[str]) -> None:
        """Function to delete the objects from a bucket.

        Args:
          bucket: Bucket name.
          paths: Paths to locate the objects in bucket.
        """
