from __future__ import annotations

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    load_dotenv()

    bucket_name: str = os.getenv("BRASILEIRAO_BUCKET", "brasileirao")
    bronze_raw_prefix: str = os.getenv("BRONZE_RAW_PREFIX", "bronze/raw")
    bronze_metadata_prefix: str = os.getenv("BRONZE_METADATA_PREFIX", "bronze/metadata")
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")
    aws_region: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    s3_endpoint_url: str = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    silver_prefix: str = field(default_factory=lambda: os.getenv("SILVER_PREFIX", "silver"))


DEFAULT_SOURCE_FILES = {
    "matches": "campeonato-brasileiro-full.csv",
    "match_statistics": "campeonato-brasileiro-estatisticas-full.csv",
    "goals": "campeonato-brasileiro-gols.csv",
    "cards": "campeonato-brasileiro-cartoes.csv",
}
