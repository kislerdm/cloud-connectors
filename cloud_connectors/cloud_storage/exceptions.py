# Dmitry Kisler © 2020-present
# www.dkisler.com

class ConfigurationError(Exception):
    """Raised when wrong config provided."""
    pass

class ObjectNotFound(Exception):
    """Raised when not object found in bucket."""
    pass


class BucketNotFound(Exception):
    """Raised when not object found in bucket."""
    pass
