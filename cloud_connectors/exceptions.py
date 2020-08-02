# Dmitry Kisler © 2020-present
# www.dkisler.com


class ConfigurationError(Exception):
    """Raised when wrong config provided."""


class ObjectNotFound(Exception):
    """Raised when not object found in bucket."""


class BucketNotFound(Exception):
    """Raised when not object found in bucket."""


class DestinationPathError(Exception):
    """Raised when cannot download object to a specified location."""


class DestinationPathPermissionsError(Exception):
    """Raised when cannot download object to a specified directory."""


class DataStructureError(Exception):
    """Raised when wrong data structure provided."""


class DatabaseConnectionError(Exception):
    """Raise when DB connection error occurred."""


class DatabaseError(Exception):
    """Raise when DB backend/client error occurred."""
