# pylint: disable=missing-function-docstring
import sys
import inspect
import warnings
import logging
from google.cloud import storage
import mock
from cloud_connectors.gcp import gcs as module


logging.basicConfig(level=logging.ERROR, format="[line: %(lineno)s] %(message)s")
LOGGER = logging.getLogger(__name__)
warnings.simplefilter(action="ignore", category=FutureWarning)

CLASSES = {"Client"}

CLASS_METHODS = {
    "CLIENT_CONFIG_SCHEMA",
    "list_buckets",
    "list_objects",
    "list_objects_size",
    "read",
    "write",
    "upload",
    "download",
    "copy",
    "move",
    "delete_object",
    "delete_objects",
}


def test_module_miss_classes() -> None:
    missing = CLASSES.difference(set(module.__dir__()))
    if missing:
        LOGGER.error(f"""Class(es) '{"', '".join(missing)}' is(are) missing.""")
        sys.exit(1)


def test_class_client_miss_methods() -> None:
    model_members = inspect.getmembers(module.Client)
    missing = CLASS_METHODS.difference({i[0] for i in model_members})
    if missing:
        LOGGER.error(f"""Class 'Client' Method(s) '{"', '".join(missing)}' is(are) missing.""")
        sys.exit(1)


module.Client.__abstractmethods__ = set()


def test_init() -> None:
    try:
        _ = module.Client()
    except Exception as ex:
        LOGGER.error(ex)
        sys.exit(1)
