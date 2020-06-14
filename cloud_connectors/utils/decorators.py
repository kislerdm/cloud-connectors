# Dmitry Kisler © 2020-present
# www.dkisler.com

from functools import wraps
import time


def connection_retry(max_attempts: int = 10):
    """Decorator to retry connection to a service endpoint."""
    def attempt(func):
        wraps(func)
        
        def wrapper(*args, **kwargs):
            for conn_attempt in range(max_attempts):
                try:
                    connection = func(*args, **kwargs)
                    if connection:
                        return connection
                except Exception as ex:
                    delay = 5 + 0.5 * conn_attempt
                    time.sleep(delay)
        return wrapper
    return attempt
