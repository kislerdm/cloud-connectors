# pylint: disable=missing-function-docstring
import sys
import warnings
import logging
from cloud_connectors import exceptions as module


logging.basicConfig(level=logging.INFO, format="[line: %(lineno)s] %(message)s")
LOGGER = logging.getLogger(__name__)
warnings.simplefilter(action="ignore", category=FutureWarning)

CLASSES = {
    "ConfigurationError",
    "ObjectNotFound",
    "BucketNotFound",
    "DestinationPathError",
    "DestinationPathPermissionsError",
    "DataStructureError",
    "DatabaseConnectionError",
    "DatabaseError",
}


def test_module_miss_classes() -> None:
    missing = CLASSES.difference(set(module.__dir__()))
    if missing:
        LOGGER.error(f"""Class(es) '{"', '".join(missing)}' is(are) missing.""")
        sys.exit(1)
