"""
Parquet storage layer :)

Writes DataFrames to partitioned Parquet files.

Merges new data with existing data, duplication by primary key.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

#parquet file paths which is relative to data_dir

FILE_SNAPSHOTS = "outage_snapshots.parquet"
FILE_US_TOTALS = "us_totals.parquet"
FILE_REFRESH_LOG = "refresh_log.parquet"

class ParquetWriter:

    """
    Manages Parquet files for the nuclear outages pipeline.

    Handles upsert semantics: new records are merged with existing data,
    deduplicating by primary key so incremental runs stay idempotent.
    """

    def __init__(self, data_dir: str | Path="data"):

        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        logger.info("ParquetWritter ready - data dir: %s", self.data_dir.resolve())

    def _path(self, file_name: str) ->Path:
        return self.data_dir / file_name
    
    def _upsert(self, new_df: pd.DataFrame, file_name: str, pk: str) -> pd.DataFrame:
        """
        Merge new_df into existing Parquet file, deduplicating on `pk`.
        New records win over existing ones for the same key.
        """
        path = self._path(file_name)
 
        if path.exists():
            existing = pd.read_parquet(path)
            # Drop existing rows whose PK appears in new data (new wins)
            existing = existing[~existing[pk].isin(new_df[pk])]
            merged = pd.concat([existing, new_df], ignore_index=True)
        else:
            merged = new_df
 
        merged.to_parquet(path, index=False, compression="snappy")
        logger.info("Wrote %d rows to %s (total after merge)", len(merged), file_name)
        return merged
 
    def write_snapshots(self, df: pd.DataFrame) -> None:
        """Write/merge outage snapshot rows."""
        if df.empty:
            return
        # Ensure date column is proper type
        df = df.copy()
        df["report_date"] = pd.to_datetime(df["report_date"]).dt.date
        #para el problema de Nan
        df = df.where(df.notna(), None)
        self._upsert(df, FILE_SNAPSHOTS, pk="snapshot_id")
 
    def write_us_totals(self, df: pd.DataFrame) -> None:
        """Write/merge US-level total rows."""
        if df.empty:
            return
        df = df.copy()
        df["report_date"] = pd.to_datetime(df["report_date"]).dt.date
        df = df.where(df.notna, None)
        self._upsert(df, FILE_US_TOTALS, pk="report_date")
 
    def append_refresh_log(self, record: dict) -> None:
        """Append a single refresh log entry (no deduplication — always append)."""
        path = self._path(FILE_REFRESH_LOG)
        new_row = pd.DataFrame([record])
 
        if path.exists():
            existing = pd.read_parquet(path)
            merged = pd.concat([existing, new_row], ignore_index=True)
        else:
            merged = new_row
 
        merged.to_parquet(path, index=False, compression="snappy")
        logger.debug("Refresh log updated (%d entries)", len(merged))
 
    def read_snapshots(self) -> pd.DataFrame:
        """Read all stored outage snapshots."""
        path = self._path(FILE_SNAPSHOTS)
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)
 
    def read_us_totals(self) -> pd.DataFrame:
        """Read all stored US totals."""
        path = self._path(FILE_US_TOTALS)
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)
 
    def read_refresh_log(self) -> pd.DataFrame:
        """Read the full refresh history."""
        path = self._path(FILE_REFRESH_LOG)
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)
 