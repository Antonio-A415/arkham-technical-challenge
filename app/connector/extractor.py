"""
Extraction orchestrator.
Coordinates EIAClient → validator → storage.

Real EIA API field names (confirmed from API dashboard):
  Generator level: period, facility, facilityName, generator, capacity, outage, percentOutage
  Facility level:  period, facility, facilityName, capacity, outage, percentOutage
  US level:        period, capacity, outage, percentOutage

Supports incremental extraction (only new dates since last successful run).
"""
import hashlib
import logging
from datetime import date, timedelta
from typing import Callable

import pandas as pd

from connector.eia_client import EIAClient, EIAAuthError, EIANetworkError
from connector.validator import validate_batch
from storage.parquet_writer import ParquetWriter
from storage.duckdb_engine import DuckDBEngine

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ID helpers
# ---------------------------------------------------------------------------

def _make_snapshot_id(facility: str, generator: str | None, period: str, level: str) -> str:
    """
    Deterministic, collision-resistant ID for a snapshot row.
    Using MD5 of the natural key — idempotent across re-runs so upserts
    don't create duplicate rows.
    """
    key = f"{facility}|{generator or ''}|{period}|{level}"
    return hashlib.md5(key.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Record mappers — one per EIA route
# ---------------------------------------------------------------------------

def _map_generator_record(raw: dict) -> dict:
    """
    Map a raw generator-level EIA record to our unified snapshot schema.

    Real API fields:
      period        -> report_date
      facility      -> plant_id  (numeric string, e.g. "46")
      facilityName  -> plant_name (e.g. "Browns Ferry")
      generator     -> generator_id (numeric string, e.g. "1", "2")
      capacity      -> capacity_mw
      outage        -> outage_mw
      percentOutage -> percent_outage
    """
    facility  = str(raw.get("facility", ""))
    generator = str(raw.get("generator")) if raw.get("generator") is not None else None
    period    = raw.get("period", "")

    return {
        "snapshot_id":    _make_snapshot_id(facility, generator, period, "generator"),
        "report_date":    period,
        "level":          "generator",
        "plant_id":       facility,
        "plant_name":     raw.get("facilityName"),
        "state":          raw.get("stateDescription") or raw.get("state"),
        "generator_id":   generator,
        "unit_name":      None,   # not returned at generator level by EIA
        "capacity_mw":    raw.get("capacity"),
        "outage_mw":      raw.get("outage"),
        "percent_outage": raw.get("percentOutage"),
    }


def _map_facility_record(raw: dict) -> dict:
    """
    Map a raw facility-level EIA record to our unified snapshot schema.

    Real API fields:
      period, facility (int), facilityName,
      capacity, outage, percentOutage
    """
    facility = str(raw.get("facility", ""))
    period   = raw.get("period", "")

    return {
        "snapshot_id":    _make_snapshot_id(facility, None, period, "facility"),
        "report_date":    period,
        "level":          "facility",
        "plant_id":       facility,
        "plant_name":     raw.get("facilityName"),
        "state":          raw.get("stateDescription") or raw.get("state"),
        "generator_id":   None,
        "unit_name":      None,
        "capacity_mw":    raw.get("capacity"),
        "outage_mw":      raw.get("outage"),
        "percent_outage": raw.get("percentOutage"),
    }


def _map_us_record(raw: dict) -> dict:
    """
    Map a raw US national aggregate record to the us_totals schema.

    Real API fields:
      period, capacity, outage, percentOutage
      (no facility or generator dimension)
    """
    return {
        "report_date":       raw.get("period", ""),
        "total_capacity_mw": raw.get("capacity"),
        "total_outage_mw":   raw.get("outage"),
        "percent_outage":    raw.get("percentOutage"),
    }


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------

class Extractor:
    """
    Full extraction pipeline: EIA API → Parquet/DuckDB.

    Pulls from all three EIA nuclear outage routes:
      1. generator-nuclear-outages  → outage_snapshots (level='generator')
      2. facility-nuclear-outages   → outage_snapshots (level='facility')
      3. us-nuclear-outages         → us_totals

    Incremental mode: reads MAX(report_date) from existing Parquet and only
    fetches records with period > that date, so daily refreshes download only
    the new day's data instead of the full history.
    """

    def __init__(self, writer: ParquetWriter, db: DuckDBEngine):
        self.writer = writer
        self.db = db

    # ── Incremental checkpoint ────────────────────────────────────────────

    def _get_last_extracted_date(self) -> str | None:
        """
        Return the latest report_date already in outage_snapshots, or None
        if the table is empty / doesn't exist yet.
        """
        try:
            result = self.db.query(
                "SELECT MAX(report_date)::VARCHAR AS last_date FROM {snapshots}"
            )
            last = result[0]["last_date"] if result else None
            logger.info("Checkpoint date: %s", last or "none — full extraction")
            return last
        except Exception as exc:
            logger.warning("Could not read checkpoint: %s", exc)
            return None

    # ── Generic page consumer ─────────────────────────────────────────────

    def _fetch_route(
        self,
        label: str,
        pages_gen,
        mapper_fn: Callable[[dict], dict],
        notify: Callable[[str, int], None],
    ) -> list[dict]:
        """
        Drain a paginated EIA route generator, validate each page, and map
        records to our internal schema. Returns the full list of mapped rows.
        """
        rows: list[dict] = []
        total_dropped = 0

        for page in pages_gen:
            valid, dropped = validate_batch(page)
            total_dropped += dropped
            for raw in valid:
                rows.append(mapper_fn(raw))
            notify(f"[{label}] {len(rows):,} rows fetched...", len(rows))

        if total_dropped:
            logger.warning("[%s] Dropped %d invalid records total", label, total_dropped)

        logger.info("[%s] Done — %d rows collected", label, len(rows))
        return rows

    # ── Main pipeline entry point ─────────────────────────────────────────

    def run(
        self,
        incremental: bool = True,
        progress_cb: Callable[[str, int], None] | None = None,
    ) -> dict:
        """
        Run the full extraction pipeline.

        Args:
            incremental:  Skip dates already stored (default True).
            progress_cb:  Optional callback(message, count) — used by the
                          WebSocket endpoint to stream progress to the frontend.

        Returns:
            Summary dict: { status, records_added, generator_rows,
                            facility_rows, us_totals_added, incremental,
                            start_date, error? }
        """
        def notify(msg: str, count: int = 0):
            logger.info(msg)
            if progress_cb:
                progress_cb(msg, count)

        # ── Determine date range ──────────────────────────────────────────
        start_date: str | None = None
        if incremental:
            last = self._get_last_extracted_date()
            if last:
                # Start from the day after the last stored date
                start_date = (date.fromisoformat(last) + timedelta(days=1)).isoformat()
                notify(f"Incremental mode — fetching from {start_date}")
            else:
                notify("No existing data found — running full historical extraction")

        client = EIAClient()  # reads EIA_API_KEY and EIA_BASE_URL from env

        # ── Route 1: Generator level ──────────────────────────────────────
        notify("Fetching generator-level outages...", 0)
        try:
            generator_rows = self._fetch_route(
                "generator",
                client.get_generator_outages(start_date=start_date),
                _map_generator_record,
                notify,
            )
        except EIAAuthError as exc:
            logger.error("Auth error on generator route: %s", exc)
            return {"status": "error", "error": str(exc), "records_added": 0}
        except EIANetworkError as exc:
            logger.error("Network error on generator route: %s", exc)
            return {"status": "error", "error": str(exc), "records_added": 0}

        # ── Route 2: Facility level ───────────────────────────────────────
        notify("Fetching facility-level outages...", len(generator_rows))
        try:
            facility_rows = self._fetch_route(
                "facility",
                client.get_facility_outages(start_date=start_date),
                _map_facility_record,
                notify,
            )
        except EIAAuthError as exc:
            logger.error("Auth error on facility route: %s", exc)
            return {"status": "error", "error": str(exc), "records_added": len(generator_rows)}
        except EIANetworkError as exc:
            logger.error("Network error on facility route: %s", exc)
            return {"status": "error", "error": str(exc), "records_added": len(generator_rows)}

        # ── Route 3: US national totals ───────────────────────────────────
        total_snapshot_rows = len(generator_rows) + len(facility_rows)
        notify("Fetching US national aggregate outages...", total_snapshot_rows)
        try:
            us_rows = self._fetch_route(
                "us-totals",
                client.get_us_outages(start_date=start_date),
                _map_us_record,
                notify,
            )
        except (EIAAuthError, EIANetworkError) as exc:
            # US totals are supplementary — log the error but don't abort
            logger.error("Could not fetch US totals (non-fatal): %s", exc)
            us_rows = []

        # ── Nothing new to store ──────────────────────────────────────────
        if not generator_rows and not facility_rows:
            notify("No new snapshot records to store.")
            return {
                "status":       "success",
                "records_added": 0,
                "us_totals_added": len(us_rows),
                "message":      "Snapshots already up to date",
                "incremental":  incremental,
                "start_date":   start_date,
            }

        # ── Write snapshots ───────────────────────────────────────────────
        all_snapshot_rows = generator_rows + facility_rows
        df_snapshots = pd.DataFrame(all_snapshot_rows).drop_duplicates("snapshot_id")
        notify(f"Writing {len(df_snapshots):,} snapshot rows to Parquet...", len(df_snapshots))
        self.writer.write_snapshots(df_snapshots)

        # ── Write US totals ───────────────────────────────────────────────
        us_totals_added = 0
        if us_rows:
            df_us = pd.DataFrame(us_rows).drop_duplicates("report_date")
            self.writer.write_us_totals(df_us)
            us_totals_added = len(df_us)
            notify(f"Wrote {us_totals_added} US total rows.", us_totals_added)

        notify(
            f"Extraction complete — "
            f"{len(generator_rows):,} generator + {len(facility_rows):,} facility "
            f"= {len(df_snapshots):,} snapshots | {us_totals_added} US totals",
            len(df_snapshots),
        )

        return {
            "status":          "success",
            "records_added":   len(df_snapshots),
            "generator_rows":  len(generator_rows),
            "facility_rows":   len(facility_rows),
            "us_totals_added": us_totals_added,
            "records_dropped": 0,
            "incremental":     incremental,
            "start_date":      start_date,
        }