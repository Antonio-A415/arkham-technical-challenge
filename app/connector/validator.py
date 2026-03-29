"""
Validate raw EIA API records before stage.

Logs warnings for bad rows instead of crashing -- pipeline continues
"""

import logging
from typing import Any


logger = logging.getLogger(__name__)

# Fields the EIA API must return for each nuclear outage record
# Confirmed from real API response: period, facility, facilityName, generator
REQUIRED_FIELDS = {"period", "facility", "facilityName"}
 
# Numeric fields we want to coerce to float (may arrive as strings or None)
NUMERIC_FIELDS = {
    "capacity",
    "outage",
    "percentOutage",
}

def _safe_float(value: Any) -> float | None:
    """Coerce a value to float, returning None if not possible."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
 
 
def validate_record(record: dict) -> dict | None:
    """
    Validate and clean a single raw EIA record.
 
    Returns the cleaned record, or None if the record must be dropped.
    Missing optional fields are set to None rather than raising.
    """
    # Check required fields are present and non-empty
    missing = [f for f in REQUIRED_FIELDS if not record.get(f)]
    if missing:
        logger.warning("Dropping record — missing required fields %s: %s", missing, record)
        return None
 
    # Coerce all numeric fields in-place
    cleaned = dict(record)
    for field in NUMERIC_FIELDS:
        if field in cleaned:
            cleaned[field] = _safe_float(cleaned[field])
 
    return cleaned
 
 
def validate_batch(records: list[dict]) -> tuple[list[dict], int]:
    """
    Validate a list of raw records.
 
    Returns:
        (valid_records, dropped_count)
    """
    valid = []
    dropped = 0
 
    for record in records:
        result = validate_record(record)
        if result is not None:
            valid.append(result)
        else:
            dropped += 1
 
    if dropped:
        logger.warning("Dropped %d invalid records out of %d", dropped, len(records))
 
    return valid, dropped
 