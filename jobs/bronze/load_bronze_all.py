from __future__ import annotations

import argparse
import json
from pathlib import Path

from jobs.bronze.bronze_loader import load_raw_file
from jobs.config import DEFAULT_SOURCE_FILES, AppConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load all Brasileirão CSV sources into the bronze layer."
    )
    parser.add_argument(
        "--data-dir",
        default=".",
        help="Directory containing the raw CSV files.",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="Override the target bucket name.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = AppConfig(bucket_name=args.bucket) if args.bucket else AppConfig()
    data_dir = Path(args.data_dir)

    results: list[dict] = []
    for source_name, filename in DEFAULT_SOURCE_FILES.items():
        file_path = data_dir / filename
        result = load_raw_file(
            file_path=file_path, source_name=source_name, config=config
        )
        results.append(result)

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
