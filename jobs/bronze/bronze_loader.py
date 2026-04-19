from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jobs.config import AppConfig
from jobs.storage import S3Storage


@dataclass(frozen=True)
class FileMetadata:
    source_name: str
    original_filename: str
    local_path: str
    s3_bucket: str
    raw_s3_key: str
    metadata_s3_key: str
    file_size_bytes: int
    row_count: int
    column_count: int
    columns: list[str]
    file_sha256: str
    ingested_at_utc: str
    load_date: str


def sha256sum(file_path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as file_obj:
        while True:
            chunk = file_obj.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def read_csv_headers_and_row_count(file_path: Path) -> tuple[list[str], int]:
    encodings_to_try = ["utf-8-sig", "utf-8", "latin-1"]
    last_error: Exception | None = None

    for encoding in encodings_to_try:
        try:
            with file_path.open("r", encoding=encoding, newline="") as file_obj:
                reader = csv.reader(file_obj)
                headers = next(reader)
                row_count = sum(1 for _ in reader)
                return headers, row_count
        except Exception as exc:  # pragma: no cover - defensive fallback
            last_error = exc

    raise RuntimeError(f"Unable to read CSV metadata for {file_path}") from last_error


def build_s3_keys(
    source_name: str, original_filename: str, load_date: str, config: AppConfig
) -> tuple[str, str]:
    raw_key = f"{config.bronze_raw_prefix}/source={source_name}/load_date={load_date}/{original_filename}"
    metadata_filename = f"{Path(original_filename).stem}.metadata.json"
    metadata_key = f"{config.bronze_metadata_prefix}/source={source_name}/load_date={load_date}/{metadata_filename}"
    return raw_key, metadata_key


def build_metadata(
    file_path: Path, source_name: str, config: AppConfig
) -> FileMetadata:
    now = datetime.now(timezone.utc)
    load_date = now.date().isoformat()
    raw_key, metadata_key = build_s3_keys(
        source_name=source_name,
        original_filename=file_path.name,
        load_date=load_date,
        config=config,
    )
    columns, row_count = read_csv_headers_and_row_count(file_path)

    return FileMetadata(
        source_name=source_name,
        original_filename=file_path.name,
        local_path=str(file_path.resolve()),
        s3_bucket=config.bucket_name,
        raw_s3_key=raw_key,
        metadata_s3_key=metadata_key,
        file_size_bytes=file_path.stat().st_size,
        row_count=row_count,
        column_count=len(columns),
        columns=columns,
        file_sha256=sha256sum(file_path),
        ingested_at_utc=now.isoformat(),
        load_date=load_date,
    )


def load_raw_file(
    file_path: Path, source_name: str, config: AppConfig
) -> dict[str, Any]:
    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    storage = S3Storage(config)
    storage.ensure_bucket_exists()

    metadata = build_metadata(
        file_path=file_path, source_name=source_name, config=config
    )
    metadata_payload = asdict(metadata)

    storage.upload_file(str(file_path), metadata.raw_s3_key)
    storage.upload_json(metadata_payload, metadata.metadata_s3_key)

    return metadata_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load a raw Brasileirão CSV file into the bronze layer."
    )
    parser.add_argument(
        "--source-name",
        required=True,
        help="Logical source name in English, e.g. matches",
    )
    parser.add_argument("--file-path", required=True, help="Local path to the CSV file")
    parser.add_argument(
        "--bucket", default=None, help="Override the target bucket name"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = AppConfig(bucket_name=args.bucket) if args.bucket else AppConfig()
    result = load_raw_file(
        file_path=Path(args.file_path),
        source_name=args.source_name,
        config=config,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
