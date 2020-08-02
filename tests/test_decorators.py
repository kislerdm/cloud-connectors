# pylint: disable=missing-function-docstring
import sys
import warnings
import logging
from cloud_connectors import decorators as module


logging.basicConfig(level=logging.INFO, format="[line: %(lineno)s] %(message)s")
LOGGER = logging.getLogger(__name__)
warnings.simplefilter(action="ignore", category=FutureWarning)

FUNCTIONS = {"connection_retry"}


def test_module_miss_classes() -> None:
    missing = FUNCTIONS.difference(set(module.__dir__()))
    if missing:
        LOGGER.error(f"""Function(s) '{"', '".join(missing)}' is(are) missing.""")
        sys.exit(1)


def test_connection_retry() -> None:
    # happy path
    @module.connection_retry(max_attempts=1)
    def test() -> int:
        return 1

    if test() != 1:
        LOGGER.error("Connection retry error.")
        sys.exit(1)

    @module.connection_retry(max_attempts=1, delay_base=0.1)
    def test_retry() -> int:
        raise Exception("test")

    if test_retry():
        LOGGER.error("Connection retry error.")
        sys.exit(1)
