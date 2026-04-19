from __future__ import annotations

import json
from io import BytesIO
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from jobs.config import AppConfig


class S3Storage:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.client: BaseClient = boto3.client(
            "s3",
            endpoint_url=config.s3_endpoint_url,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.aws_region,
        )

    def ensure_bucket_exists(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.config.bucket_name)
        except ClientError:
            self.client.create_bucket(Bucket=self.config.bucket_name)

    def upload_file(
        self, local_path: str, key: str, content_type: str = "text/csv"
    ) -> None:
        extra_args = {"ContentType": content_type}
        self.client.upload_file(
            local_path, self.config.bucket_name, key, ExtraArgs=extra_args
        )

    def upload_json(self, payload: dict[str, Any], key: str) -> None:
        raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.client.upload_fileobj(
            Fileobj=BytesIO(raw),
            Bucket=self.config.bucket_name,
            Key=key,
            ExtraArgs={"ContentType": "application/json"},
        )
