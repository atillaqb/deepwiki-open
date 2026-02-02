import json
import logging
import os
from datetime import datetime
from functools import lru_cache
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

STORAGE_BACKEND = os.environ.get("DEEPWIKI_STORAGE_BACKEND", "local").lower()
S3_ENABLED_ENV = os.environ.get("DEEPWIKI_S3_ENABLED", "").lower() in ["1", "true", "yes"]
S3_BUCKET = os.environ.get("DEEPWIKI_S3_BUCKET")
S3_PREFIX = os.environ.get("DEEPWIKI_S3_PREFIX", "deepwiki")
S3_ENDPOINT_URL = os.environ.get("DEEPWIKI_S3_ENDPOINT_URL")
S3_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")


def s3_enabled() -> bool:
    return bool(S3_BUCKET) and (STORAGE_BACKEND == "s3" or S3_ENABLED_ENV)


def build_s3_key(*parts: str) -> str:
    cleaned_parts = []
    for part in parts:
        if not part:
            continue
        normalized = part.replace(os.sep, "/").strip("/")
        if normalized:
            cleaned_parts.append(normalized)
    key = "/".join(cleaned_parts)
    prefix = (S3_PREFIX or "").strip("/")
    if prefix:
        return f"{prefix}/{key}" if key else prefix
    return key


@lru_cache(maxsize=1)
def _s3_client():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT_URL, region_name=S3_REGION)


def s3_object_exists(key: str) -> bool:
    if not s3_enabled():
        return False
    try:
        _s3_client().head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ["404", "NoSuchKey", "NotFound"]:
            return False
        logger.error("S3 head_object failed for %s: %s", key, exc)
        return False


def s3_read_json(key: str) -> Optional[Dict]:
    if not s3_enabled():
        return None
    try:
        response = _s3_client().get_object(Bucket=S3_BUCKET, Key=key)
        payload = response["Body"].read().decode("utf-8")
        return json.loads(payload)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ["404", "NoSuchKey", "NotFound"]:
            return None
        logger.error("S3 get_object failed for %s: %s", key, exc)
        return None
    except Exception as exc:
        logger.error("S3 read_json failed for %s: %s", key, exc)
        return None


def s3_write_json(key: str, data: Dict) -> bool:
    if not s3_enabled():
        return False
    try:
        payload = json.dumps(data, ensure_ascii=True).encode("utf-8")
        _s3_client().put_object(Bucket=S3_BUCKET, Key=key, Body=payload)
        return True
    except Exception as exc:
        logger.error("S3 write_json failed for %s: %s", key, exc)
        return False


def s3_download_file(key: str, local_path: str) -> bool:
    if not s3_enabled():
        return False
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        _s3_client().download_file(S3_BUCKET, key, local_path)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ["404", "NoSuchKey", "NotFound"]:
            return False
        logger.error("S3 download_file failed for %s: %s", key, exc)
        return False
    except Exception as exc:
        logger.error("S3 download_file failed for %s: %s", key, exc)
        return False


def s3_upload_file(local_path: str, key: str) -> bool:
    if not s3_enabled():
        return False
    if not os.path.exists(local_path):
        return False
    try:
        _s3_client().upload_file(local_path, S3_BUCKET, key)
        return True
    except Exception as exc:
        logger.error("S3 upload_file failed for %s: %s", key, exc)
        return False


def s3_delete_object(key: str) -> bool:
    if not s3_enabled():
        return False
    try:
        _s3_client().delete_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception as exc:
        logger.error("S3 delete_object failed for %s: %s", key, exc)
        return False


def s3_list_objects(prefix: str) -> List[Dict]:
    if not s3_enabled():
        return []
    client = _s3_client()
    paginator = client.get_paginator("list_objects_v2")
    items: List[Dict] = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for entry in page.get("Contents", []):
            items.append(
                {
                    "key": entry["Key"],
                    "last_modified": entry.get("LastModified"),
                    "size": entry.get("Size", 0),
                }
            )
    return items


def ensure_local_file(local_path: str, key: str) -> bool:
    if os.path.exists(local_path):
        return True
    if not s3_enabled():
        return False
    if not s3_object_exists(key):
        return False
    return s3_download_file(key, local_path)


def parse_s3_last_modified(value: Optional[datetime]) -> Optional[int]:
    if not value:
        return None
    try:
        return int(value.timestamp() * 1000)
    except Exception:
        return None
