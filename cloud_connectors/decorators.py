# Dmitry Kisler © 2020-present
# www.dkisler.com

from typing import Callable
from functools import wraps
import time


def connection_retry(
    max_attempts: int = 10, delay_base: float = 5., delay_increment: float = 0.5
) -> Callable:
    """Decorator to retry connection to a service endpoint."""

    def attempt(func):
        wraps(func)

        def wrapper(*args, **kwargs):
            for conn_attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    delay = delay_base + delay_increment * conn_attempt
                    time.sleep(delay)

        return wrapper

    return attempt
