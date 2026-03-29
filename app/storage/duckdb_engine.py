"""
DuckDB query engine :)

Runs SQL directly on Parquet files, no ETL to a separate DB needed.

DuckDB reads Parquet natively and is very fast for analytical queries.
"""
import logging
from pathlib import Path
from typing import Any

import duckdb

logger = logging.getLogger(__name__)


class DuckDBEngine:
    """
    Provides SQL query access over Parquet files using DuckDB.

    Each query opens a fresh in-memory connection that scans the Parquet files.

    This keeps the engine stateless and safe for concurrent FastAPI requests.
    """

    def __init__(self, data_dir: str | Path = "data"):
        
        self.data_dir = Path(data_dir)
        logger.info("DuckDBEngine ready — scanning: %s", self.data_dir.resolve())

    def _parquet(self, filename: str) -> str:
        """Return a DuckDB-compatible path expression for a Parquet file."""
        path = self.data_dir / filename
        return f"read_parquet('{path}')"

    def query(self, sql: str, params: list[Any] | None = None) -> list[dict]:
        """
        Execute a raw SQL query and return results as a list of dicts.
        Parquet file names are injected via the {snapshots}, {us_totals}, {log} placeholders.
        """
        # Inject table references so callers don't need to know file paths
        sql = sql.format(
            snapshots=self._parquet("outage_snapshots.parquet"),
            us_totals=self._parquet("us_totals.parquet"),
            log=self._parquet("refresh_log.parquet"),
        )

        conn = duckdb.connect(":memory:")
        try:
            if params:
                result = conn.execute(sql, params).fetchdf()
            else:
                result = conn.execute(sql).fetchdf()
            return result.to_dict(orient="records")
        except duckdb.IOException as exc:
            # Parquet file doesn't exist yet (no data ingested)
            logger.warning("DuckDB query failed (no data?): %s", exc)
            return []
        finally:
            conn.close()

    # -------------------------------------------------------------------------
    # Pre-built queries used by the API routers
    # -------------------------------------------------------------------------

    def get_outages(
        self,
        level: str | None = None,
        plant_id: str | None = None,
        state: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        Flexible outage query with optional filters.
        Returns paginated results sorted by report_date DESC.
        """
        conditions = ["1=1"]
        params = []

        if level:
            conditions.append("level = ?")
            params.append(level)
        if plant_id:
            conditions.append("plant_id = ?")
            params.append(plant_id)
        if state:
            conditions.append("UPPER(state) = UPPER(?)")
            params.append(state)
        if start_date:
            conditions.append("report_date >= ?::DATE")
            params.append(start_date)
        if end_date:
            conditions.append("report_date <= ?::DATE")
            params.append(end_date)

        where = " AND ".join(conditions)
        sql = f"""
            SELECT *
            FROM {{snapshots}}
            WHERE {where}
            ORDER BY report_date DESC, plant_name
            LIMIT {limit} OFFSET {offset}
        """
        return self.query(sql, params or None)

    def count_outages(
        self,
        level: str | None = None,
        plant_id: str | None = None,
        state: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> int:
        """Return total count matching the same filters as get_outages (for pagination metadata)."""
        conditions = ["1=1"]
        params = []

        if level:
            conditions.append("level = ?")
            params.append(level)
        if plant_id:
            conditions.append("plant_id = ?")
            params.append(plant_id)
        if state:
            conditions.append("UPPER(state) = UPPER(?)")
            params.append(state)
        if start_date:
            conditions.append("report_date >= ?::DATE")
            params.append(start_date)
        if end_date:
            conditions.append("report_date <= ?::DATE")
            params.append(end_date)

        where = " AND ".join(conditions)
        result = self.query(
            f"SELECT COUNT(*) as n FROM {{snapshots}} WHERE {where}",
            params or None,
        )
        return result[0]["n"] if result else 0

    def get_analytics(self) -> dict:
        """
        Pre-computed analytics for the dashboard:
          - Top 10 plants by average outage MW
          - Monthly US outage trend
          - Current snapshot (latest date)
        """
        top_plants = self.query("""
            SELECT plant_name, plant_id, state,
                   ROUND(AVG(outage_mw), 1) AS avg_outage_mw,
                   ROUND(AVG(percent_outage), 1) AS avg_pct_outage,
                   COUNT(*) AS data_points
            FROM {snapshots}
            WHERE level = 'facility' AND outage_mw IS NOT NULL AND outage_mw > 0
            GROUP BY plant_name, plant_id, state
            ORDER BY avg_outage_mw DESC
            LIMIT 10
        """)

        monthly_trend = self.query("""
            SELECT DATE_TRUNC('month', report_date::TIMESTAMP)::DATE AS month,
                   ROUND(AVG(total_outage_mw), 1) AS avg_outage_mw,
                   ROUND(AVG(percent_outage), 2) AS avg_pct_outage
            FROM {us_totals}
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 24
        """)

        latest = self.query("""
            SELECT report_date, total_capacity_mw, total_outage_mw, percent_outage
            FROM {us_totals}
            ORDER BY report_date DESC
            LIMIT 1
        """)

        return {
            "top_plants_by_outage": top_plants,
            "monthly_us_trend": monthly_trend,
            "latest_us_snapshot": latest[0] if latest else None,
        }

    def get_refresh_log(self, limit: int = 20) -> list[dict]:
        """Return the most recent refresh runs."""
        return self.query(f"""
            SELECT * FROM {{log}}
            ORDER BY triggered_at DESC
            LIMIT {limit}
        """)