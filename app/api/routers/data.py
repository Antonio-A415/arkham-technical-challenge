"""
GET /data — returns filtered, paginated outage records.
GET /analytics — returns pre-computed dashboard analytics.
"""
from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_db, verify_api_key
from api.schemas import AnalyticsResponse, PaginatedOutages
from storage.duckdb_engine import DuckDBEngine
import math


router = APIRouter(prefix="/data", tags=["data"])


def _sanitize(records: list[dict]) -> list[dict]:
    """Replace float NaN (pandas NULL sentinel) with None so Pydantic accepts it."""
    return [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]

import math
from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.dependencies import get_db, verify_api_key
from api.schemas import AnalyticsResponse, PaginatedOutages
from storage.duckdb_engine import DuckDBEngine

router = APIRouter(prefix="/data", tags=["data"])


def _sanitize(records: list[dict]) -> list[dict]:
    """Replace float NaN (pandas NULL sentinel) with None so Pydantic accepts it."""
    return [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]


@router.get("", response_model=PaginatedOutages)
def get_outages(
    level: Optional[str] = Query(None),
    plant_id: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    db: DuckDBEngine = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    records = db.get_outages(
        level=level, plant_id=plant_id, state=state,
        start_date=start_date, end_date=end_date,
        limit=limit, offset=offset,
    )
    total = db.count_outages(
        level=level, plant_id=plant_id, state=state,
        start_date=start_date, end_date=end_date,
    )
    return PaginatedOutages(total=total, limit=limit, offset=offset, data=_sanitize(records))


@router.get("", response_model=PaginatedOutages)
def get_outages_v1(
    level: Optional[str] = Query(None, description="Filter by level: facility | generator | us"),
    plant_id: Optional[str] = Query(None, description="Filter by plant ID"),
    state: Optional[str] = Query(None, description="Filter by US state abbreviation (e.g. TX)"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit: int = Query(100, ge=1, le=5000, description="Records per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db: DuckDBEngine = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """
    Return outage snapshots with optional filtering and pagination.
    Supports level (facility/generator), state, plant_id, and date range filters.
    """
    records = db.get_outages(
        level=level,
        plant_id=plant_id,
        state=state,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        offset=offset,
    )
    total = db.count_outages(
        level=level,
        plant_id=plant_id,
        state=state,
        start_date=start_date,
        end_date=end_date,
    )
    return PaginatedOutages(total=total, limit=limit, offset=offset, data=records)


@router.get("/analytics", response_model=AnalyticsResponse, tags=["analytics"])
def get_analytics(
    db: DuckDBEngine = Depends(get_db),
    _: str = Depends(verify_api_key),
):
    """
    Return pre-computed analytics:
    - Top 10 plants by average outage MW
    - Monthly US outage trend (last 24 months)
    - Latest US-level snapshot
    """
    return db.get_analytics()