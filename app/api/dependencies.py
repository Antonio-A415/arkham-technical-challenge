"""
FastAPI dependecy injection :)

Shared instances of storage/DB and api key authentication.
"""


import os
from functools import lru_cache

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from storage.parquet_writer import ParquetWriter
from storage.duckdb_engine import DuckDBEngine
from connector.extractor import Extractor

DATA_DIR = os.getenv("DATA_DIR", "data")


# Set APP_API_KEY env var to enable; leave unset to disable auth (dev mode)
APP_API_KEY = os.getenv("APP_API_KEY")

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(key: str | None = Security(api_key_header)) -> str | None:
    """
    If APP_API_KEY is configured, enforce it on all requests.
    If not set, authentication is disabled (useful for local dev).
    """
    if not APP_API_KEY:
        return None  # Auth disabled
    if key != APP_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key header.",
        )
    return key


@lru_cache(maxsize=1)
def get_writer() -> ParquetWriter:
    """Singleton Parquet writer (shared across requests)."""
    return ParquetWriter(data_dir=DATA_DIR)


@lru_cache(maxsize=1)
def get_db() -> DuckDBEngine:
    """Singleton DuckDB engine (shared across requests)."""
    return DuckDBEngine(data_dir=DATA_DIR)


def get_extractor() -> Extractor:
    """New Extractor per request (stateless, uses shared writer+db)."""
    return Extractor(writer=get_writer(), db=get_db())