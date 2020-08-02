# Dmitry Kisler © 2020-present
# www.dkisler.com
# pylint: disable=missing-function-docstring
import sys
import inspect
import warnings
import logging
from cloud_connectors.template import cloud_storage as module


logging.basicConfig(level=logging.ERROR, format="[line: %(lineno)s] %(message)s")
LOGGER = logging.getLogger(__name__)
warnings.simplefilter(action="ignore", category=FutureWarning)

CLASSES = {"Client"}

CLASS_METHODS = {
    "list_buckets",
    "list_objects",
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
    client = module.Client({})
    if client.client != "client":
        LOGGER.error("Init error")
        sys.exit(1)
