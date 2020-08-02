# pylint: disable=missing-function-docstring
import os
import sys
import json
import inspect
import warnings
import logging
from moto import mock_s3  # type: ignore
import boto3  # type: ignore
from cloud_connectors.aws import s3 as module


logging.basicConfig(level=logging.ERROR, format="[line: %(lineno)s] %(message)s")
LOGGER = logging.getLogger(__name__)
warnings.simplefilter(action="ignore", category=FutureWarning)

CLASSES = {"Client"}

CLASS_METHODS = {
    "CLIENT_CONFIG_SCHEMA",
    "S3_TRANSFER_SCHEMA",
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


def test_connection() -> None:
    tests = [
        {
            "type": "valid",
            "config": {
                "aws_access_key_id": "AKIAAAAAAAAAAAAA1111",
                "aws_secret_access_key": "aaaaaaaaxxxxxxxx02330128skjjhasdg7723s!!",
            },
        },
        {
            "type": "failty_config_key",
            "config": {
                "aws_access_key_id": "AKIAAAAAAAAAAAAA11",
                "aws_secret_access_key": "aaaaaaaaxxxxxxxx02330128skjjhasdg7723s!!",
            },
        },
        {
            "type": "failty_config_secret",
            "config": {
                "aws_access_key_id": "AKIAAAAAAAAAAAAA1111",
                "aws_secret_access_key": "aaaaaaaaxxxxxxxx02330128skjjhasdg7723s",
            },
        },
    ]

    for test in tests:
        try:
            _ = module.Client(test["config"])
        except Exception as ex:
            if test["type"] == "valid":
                LOGGER.error(f"Failed client instance: {ex}")
                sys.exit(1)

            elif test["type"] == "failty_config_key":
                if type(ex).__name__ != "ConfigurationError":
                    LOGGER.error("Wrong error type to handle config error")
                    sys.exit(1)

                if "data.aws_access_key_id" not in str(ex):
                    LOGGER.error(f"Configuration validator error - key: {ex}")
                    sys.exit(1)

            elif test["type"] == "failty_config_secret":
                if type(ex).__name__ != "ConfigurationError":
                    LOGGER.error("Wrong error type to handle config error")
                    sys.exit(1)

                if "data.aws_secret_access_key" not in str(ex):
                    LOGGER.error(f"Configuration validator error - secret: {ex}")
                    sys.exit(1)


@mock_s3
def test_list_buckets() -> None:
    tests = [
        {"create": None, "want": [],},
        {"create": "test", "want": ["test"],},
        {"create": "test1", "want": ["test", "test1"]},
    ]

    def _create_dummy_buckets(bucket: str) -> None:
        boto3.client("s3").create_bucket(Bucket=bucket)

    client = module.Client()

    for test in tests:
        if test["create"]:
            _create_dummy_buckets(test["create"])
        buckets = client.list_buckets()
        if buckets != test["want"]:
            LOGGER.error(f"Error listing buckets. got: {buckets}, want: {test['want']}")
            sys.exit(1)


BUCKET = "test_bucket"
OBJ_CONTENT = {"products": [{"a": 1}, {"b": "foo"},]}


def put_object(mock_client: boto3.client, obj_key: str) -> None:
    mock_client.put_object(
        Bucket=BUCKET, Key=obj_key, Body=json.dumps(OBJ_CONTENT), ContentType="application/json",
    )


@mock_s3
def test_list_objects() -> None:
    mock_client = boto3.client("s3")
    mock_client.create_bucket(Bucket=BUCKET)

    tests = [
        {"success": False, "create": None, "want": [],},
        {"success": True, "create": None, "want": [],},
        {"success": True, "create": "test.json", "want": ["test.json"],},
        {"success": True, "create": "test1.json", "want": ["test.json", "test1.json"],},
        {"success": True, "create": "blah.json", "want": ["test.json", "test1.json"],},
    ]

    client = module.Client()

    for test in tests:
        if test["success"]:
            if test["create"]:
                put_object(mock_client, test["create"])
            objects = client.list_objects(bucket=BUCKET, prefix="test")
            if objects != test["want"]:
                LOGGER.error(f"Error listing objects. got: {objects}, want: {test['want']}")
                sys.exit(1)
        else:
            try:
                objects = client.list_objects(bucket=f"{BUCKET}_bar", prefix="test")
            except Exception as ex:
                if type(ex).__name__ != "BucketNotFound":
                    LOGGER.error("Wrong error type to handle NoSuchBucket error")
                    sys.exit(1)


@mock_s3
def test_list_objects_size() -> None:
    mock_client = boto3.client("s3")
    mock_client.create_bucket(Bucket=BUCKET)

    tests = [
        {"success": False, "create": None, "want": [],},
        {"success": True, "create": None, "want": [],},
        {"success": True, "create": "test.json", "want": [("test.json", 38)],},
        {"success": True, "create": "test1.json", "want": [("test.json", 38), ("test1.json", 38)],},
        {"success": True, "create": "blah.json", "want": [("test.json", 38), ("test1.json", 38)],},
    ]

    client = module.Client()

    for test in tests:
        if test["success"]:
            if test["create"]:
                put_object(mock_client, test["create"])
            objects = client.list_objects_size(bucket=BUCKET, prefix="test")
            if objects != test["want"]:
                LOGGER.error(f"Error listing objects. got: {objects}, want: {test['want']}")
                sys.exit(1)
        else:
            try:
                objects = client.list_objects_size(bucket=f"{BUCKET}_bar", prefix="test")
            except Exception as ex:
                if type(ex).__name__ != "BucketNotFound":
                    LOGGER.error("Wrong error type to handle NoSuchBucket error")
                    sys.exit(1)


@mock_s3
def test_read() -> None:
    path = "test.json"

    mock_client = boto3.client("s3")
    mock_client.create_bucket(Bucket=BUCKET)

    put_object(mock_client, path)

    client = module.Client()

    try:
        _ = client.read(bucket=f"{BUCKET}_bar", path=path)
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)

    try:
        _ = client.read(bucket=BUCKET, path=f"{path}_bar")
    except Exception as ex:
        if type(ex).__name__ != "ObjectNotFound":
            LOGGER.error("Wrong error type to handle NoSuchKey error")
            sys.exit(1)

    obj = client.read(bucket=BUCKET, path=path)
    if json.loads(obj) != OBJ_CONTENT:
        LOGGER.error("Error reading object")
        sys.exit(1)

    try:
        client.read("", "test.json")
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle InvalidBucketName error")
            sys.exit(1)


@mock_s3
def test_write() -> None:
    path = "test.json"

    mock_client = boto3.client("s3")
    mock_client.create_bucket(Bucket=BUCKET)

    client = module.Client()

    try:
        client.write(obj=json.dumps(OBJ_CONTENT), bucket=f"{BUCKET}_bar", path=path)
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)

    try:
        client.write(obj=OBJ_CONTENT, bucket=BUCKET, path=path)
    except Exception as ex:
        if type(ex).__name__ != "TypeError":
            LOGGER.error("Wrong error type to handle ParamValidationError error")
            sys.exit(1)

    client.write(obj=json.dumps(OBJ_CONTENT), bucket=BUCKET, path=path)

    obj = mock_client.get_object(Bucket=BUCKET, Key=path)["Body"].read()
    if json.loads(obj) != OBJ_CONTENT:
        LOGGER.error("Error writing object")
        sys.exit(1)


@mock_s3
def test_upload() -> None:
    path = "test.json"
    path_os = f"/tmp/{path}"

    mock_client = boto3.client("s3")
    mock_client.create_bucket(Bucket=BUCKET)

    client = module.Client()

    try:
        client.upload(bucket=BUCKET, path_source=path_os, path_destination=path)
    except Exception as ex:
        if type(ex).__name__ != "FileNotFoundError":
            LOGGER.error("Wrong error type to handle FileNotFoundError error")
            sys.exit(1)

    with open(path_os, "w") as f:
        json.dump(OBJ_CONTENT, f)

    try:
        client.upload(bucket=f"{BUCKET}_bar", path_source=f"/tmp/{path}", path_destination=path)
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)

    client.upload(bucket=BUCKET, path_source=path_os, path_destination=path)

    obj = mock_client.get_object(Bucket=BUCKET, Key=path)["Body"].read()
    if json.loads(obj) != OBJ_CONTENT:
        LOGGER.error("Error writing object")
        sys.exit(1)

    os.remove(path_os)


@mock_s3
def test_download() -> None:
    path = "test.json"
    path_os = f"/tmp/{path}"

    mock_client = boto3.client("s3")
    mock_client.create_bucket(Bucket=BUCKET)
    put_object(mock_client, path)

    client = module.Client()

    try:
        client.download(bucket=f"{BUCKET}_bar", path_source=path, path_destination=path_os)
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)

    try:
        client.download(bucket=BUCKET, path_source=f"{path}_bar", path_destination=path_os)
    except Exception as ex:
        if type(ex).__name__ != "ObjectNotFound":
            LOGGER.error("Wrong error type to handle NoSuchKey error")
            sys.exit(1)

    try:
        client.download(bucket=BUCKET, path_source=path, path_destination="/bin/test.json")
    except Exception as ex:
        if type(ex).__name__ != "DestinationPathPermissionsError":
            LOGGER.error("Wrong error type to handle write permissoin error")
            sys.exit(1)

    try:
        client.download(
            bucket=BUCKET, path_source=path, path_destination="/tmp/s3_test____/test.json",
        )
    except Exception as ex:
        if type(ex).__name__ != "DestinationPathError":
            LOGGER.error("Wrong error type to handle desination directory error")
            sys.exit(1)

    client.download(bucket=BUCKET, path_source=path, path_destination=path_os)

    with open(path_os, "r") as f:
        if json.load(f) != OBJ_CONTENT:
            LOGGER.error("Error downloading object - content")
            sys.exit(1)

    os.remove(path_os)

    try:
        client.download(
            bucket=BUCKET, path_source=path, path_destination=path_os, configuration={"a": 1}
        )
    except Exception as ex:
        if type(ex).__name__ != "ConfigurationError":
            LOGGER.error("Wrong error type to handle download configuration error")
            sys.exit(1)


@mock_s3
def test_copy() -> None:
    path = "test.json"

    mock_client = boto3.client("s3")
    mock_client.create_bucket(Bucket=BUCKET)
    mock_client.create_bucket(Bucket=f"{BUCKET}_destination")
    put_object(mock_client, path)

    client = module.Client()

    try:
        client.copy(
            bucket_source=f"{BUCKET}_bar",
            bucket_destination=f"{BUCKET}_destination",
            path_source=path,
        )
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)

    try:
        client.copy(
            bucket_source=BUCKET,
            bucket_destination=f"{BUCKET}_destination",
            path_source=f"{path}_bar",
        )
    except Exception as ex:
        if type(ex).__name__ != "ObjectNotFound":
            LOGGER.error("Wrong error type to handle NoSuchKey error")
            sys.exit(1)

    try:
        client.copy(
            bucket_source=BUCKET, bucket_destination=f"{BUCKET}_destination_bar", path_source=path,
        )
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)

    client.copy(
        bucket_source=BUCKET, bucket_destination=f"{BUCKET}_destination", path_source=path,
    )

    obj = mock_client.get_object(Bucket=BUCKET, Key=path)
    if json.loads(obj["Body"].read()) != OBJ_CONTENT:
        LOGGER.error("Error copying object - content")
        sys.exit(1)

    if obj["ContentType"] != "application/json":
        LOGGER.error("Error copying object - content type")
        sys.exit(1)

    # replace the object itself
    try:
        client.copy(
            bucket_source=BUCKET,
            bucket_destination=BUCKET,
            path_source=path,
            path_destination=path,
        )
    except Exception as ex:
        LOGGER.error(ex)
        sys.exit(1)


@mock_s3
def test_delete_object() -> None:
    path = "test.json"

    mock_client = boto3.client("s3")
    mock_client.create_bucket(Bucket=BUCKET)
    put_object(mock_client, path)

    client = module.Client()

    try:
        client.delete_object(bucket=f"{BUCKET}_bar", path=path)
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)

    try:
        client.delete_object(bucket=BUCKET, path=f"{path}_bar")
    except Exception as ex:
        if type(ex).__name__ != "ObjectNotFound":
            LOGGER.error("Wrong error type to handle NoSuchKey error")
            sys.exit(1)

    client.delete_object(bucket=BUCKET, path=path)

    obj_list = mock_client.list_objects_v2(Bucket=BUCKET, Prefix=path)
    if "Contents" in obj_list:
        LOGGER.error("Error deleting object")
        sys.exit(1)


@mock_s3
def test_delete_objects() -> None:
    paths = ["test.json", "test1.json"]

    mock_client = boto3.client("s3")
    mock_client.create_bucket(Bucket=BUCKET)
    for path in paths:
        put_object(mock_client, path)

    client = module.Client()

    try:
        client.delete_objects(bucket=f"{BUCKET}_bar", paths=paths)
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)

    try:
        client.delete_objects(bucket=BUCKET, paths=[*paths, "test_bar.json"])
    except Exception as ex:
        if type(ex).__name__ != "ObjectNotFound":
            LOGGER.error("Wrong error type to handle NoSuchKey error")
            sys.exit(1)

    client.delete_objects(bucket=BUCKET, paths=paths)

    for path in paths:
        obj_list = mock_client.list_objects_v2(Bucket=BUCKET, Prefix=path)
        if "Contents" in obj_list:
            LOGGER.error("Error deleting object")
            sys.exit(1)


@mock_s3
def test_move() -> None:
    path = "test.json"

    mock_client = boto3.client("s3")
    mock_client.create_bucket(Bucket=BUCKET)
    mock_client.create_bucket(Bucket=f"{BUCKET}_destination")
    put_object(mock_client, path)

    client = module.Client()

    try:
        client.move(
            bucket_source=f"{BUCKET}_bar",
            bucket_destination=f"{BUCKET}_destination",
            path_source=path,
        )
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)

    try:
        client.move(
            bucket_source=BUCKET,
            bucket_destination=f"{BUCKET}_destination",
            path_source=f"{path}_bar",
        )
    except Exception as ex:
        if type(ex).__name__ != "ObjectNotFound":
            LOGGER.error("Wrong error type to handle NoSuchKey error")
            sys.exit(1)

    try:
        client.move(
            bucket_source=BUCKET, bucket_destination=f"{BUCKET}_destination_bar", path_source=path,
        )
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)

    client.move(
        bucket_source=BUCKET, bucket_destination=f"{BUCKET}_destination", path_source=path,
    )

    obj_list_source = mock_client.list_objects_v2(Bucket=BUCKET, Prefix=path)
    obj_list_destination = mock_client.list_objects_v2(Bucket=f"{BUCKET}_destination", Prefix=path)

    if "Contents" in obj_list_source and len(obj_list_destination["Contents"]) == 1:
        LOGGER.error("Error moving object")
        sys.exit(1)

    obj = mock_client.get_object(Bucket=f"{BUCKET}_destination", Key=path)
    if json.loads(obj["Body"].read()) != OBJ_CONTENT:
        LOGGER.error("Error copying object - content")
        sys.exit(1)

    if obj["ContentType"] != "application/json":
        LOGGER.error("Error copying object - content type")
        sys.exit(1)


def test_exceptions() -> None:
    client = module.Client()

    try:
        client.list_buckets()
    except Exception as ex:
        if type(ex).__name__ != "ConnectionError":
            LOGGER.error("Wrong error type to handle access error")
            sys.exit(1)

    try:
        client.list_objects("")
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)
    try:
        client.list_objects("t")
    except Exception as ex:
        if type(ex).__name__ != "BucketNotFound":
            LOGGER.error("Wrong error type to handle NoSuchBucket error")
            sys.exit(1)
