import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()


def get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def get_connection():
    return psycopg2.connect(
        host=get_env("POSTGRES_HOST", "localhost"),
        port=get_env("POSTGRES_PORT", "5432"),
        dbname=get_env("POSTGRES_DB", "brasileirao"),
        user=get_env("POSTGRES_USER", "postgres"),
        password=get_env("POSTGRES_PASSWORD", "postgres"),
        cursor_factory=RealDictCursor,
    )
