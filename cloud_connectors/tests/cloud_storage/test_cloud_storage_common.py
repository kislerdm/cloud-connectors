# Dmitry Kisler © 2020-present
# www.dkisler.com

import pathlib
import importlib.util
from types import ModuleType
import warnings


warnings.simplefilter(action='ignore',
                      category=FutureWarning)

DIR = pathlib.Path(__file__).absolute().parents
PACKAGE = "cloud_storage"
MODULE = "common"

CLASSES = set(['Client'])

CLASS_ATTS = set(["ls", "mv",
                  "read", "store",
                  "upload", "download",
                  "delete_object", "delete_bucket"])


def load_module(module_name: str) -> ModuleType:
    """Function to load the module.
    
    Args:
      module_name: Module name.
    
    Returns:
      Module object.
    """
    file_path = f"{DIR[2]}/{PACKAGE}/{module_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_module_exists():
    try:
        _ = load_module(MODULE)
    except Exception as ex:
        raise ex
    return


module = load_module(MODULE)


def test_module_miss_classes() -> None:
    missing = CLASSES.difference(set(module.__dir__()))
    assert not missing, f"""Class(es) '{"', '".join(missing)}' is(are) missing."""
    return
