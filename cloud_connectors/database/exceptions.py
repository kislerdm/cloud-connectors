# Dmitry Kisler © 2020-present
# www.dkisler.com

class ConfigurationError(Exception):
    """Raised when wrong config provided."""
    pass


class DataStructureError(Exception):
    """Raised when wrong data structure provided."""
    pass


class DatabaseConnectionError(Exception):
    """Raise when DB connection error occurred."""
    pass
  

class DatabaseError(Exception):
    """Raise when DB backend/client error occurred."""
    pass
