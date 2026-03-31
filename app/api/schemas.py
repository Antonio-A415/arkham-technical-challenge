"""
Pydantic models for FastAPI request validation and response serialization.
"""
from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# /data endpoint
# ---------------------------------------------------------------------------

class OutageRecord(BaseModel):
    snapshot_id: str
    report_date: date
    level: str
    plant_id: Optional[str] = None
    plant_name: Optional[str] = None
    state: Optional[str] = None
    generator_id: Optional[str] = None
    unit_name: Optional[str] = None
    capacity_mw: Optional[float] = None
    outage_mw: Optional[float] = None
    percent_outage: Optional[float] = None

    class Config:
        from_attributes = True


class PaginatedOutages(BaseModel):
    total: int
    limit: int
    offset: int
    data: list[OutageRecord]


# ---------------------------------------------------------------------------
# /refresh endpoint
# ---------------------------------------------------------------------------

class RefreshResponse(BaseModel):
    status: str
    records_added: int = 0
    us_totals_added: int = 0
    records_dropped: int = 0
    incremental: bool = True
    start_date: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# /analytics endpoint
# ---------------------------------------------------------------------------

class AnalyticsResponse(BaseModel):
    top_plants_by_outage: list[dict[str, Any]]
    monthly_us_trend: list[dict[str, Any]]
    latest_us_snapshot: Optional[dict[str, Any]] = None


# ---------------------------------------------------------------------------
# /health endpoint
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str
    data_dir: str
    snapshots_available: bool
    us_totals_available: bool


# ---------------------------------------------------------------------------
# WebSocket progress event
# ---------------------------------------------------------------------------

class ProgressEvent(BaseModel):
    event: str = "progress"
    message: str
    count: int = 0