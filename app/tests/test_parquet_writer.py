
from datetime import date

import pandas as pd
# ---------------------------------------------------------------------------
# ParquetWriter tests
# ---------------------------------------------------------------------------

class TestParquetWriter:
    def test_write_and_read_snapshots(self, tmp_path):
        from storage.parquet_writer import ParquetWriter
        writer = ParquetWriter(data_dir=tmp_path)

        df = pd.DataFrame([{
            "snapshot_id": "abc123",
            "report_date": date(2024, 1, 15),
            "level": "facility",
            "plant_id": "P1",
            "plant_name": "Test Plant",
            "state": "TX",
            "generator_id": None,
            "unit_name": None,
            "capacity_mw": 1200.0,
            "outage_mw": 0.0,
            "percent_outage": 0.0,
        }])

        writer.write_snapshots(df)
        result = writer.read_snapshots()
        assert len(result) == 1
        assert result.iloc[0]["plant_name"] == "Test Plant"

    def test_upsert_deduplicates_by_pk(self, tmp_path):
        from storage.parquet_writer import ParquetWriter
        writer = ParquetWriter(data_dir=tmp_path)

        base = {
            "snapshot_id": "same_id",
            "report_date": date(2024, 1, 1),
            "level": "facility",
            "plant_id": "P1",
            "plant_name": "Original",
            "state": None,
            "generator_id": None,
            "unit_name": None,
            "capacity_mw": None,
            "outage_mw": None,
            "percent_outage": None,
        }

        writer.write_snapshots(pd.DataFrame([base]))
        updated = {**base, "plant_name": "Updated"}
        writer.write_snapshots(pd.DataFrame([updated]))

        result = writer.read_snapshots()
        assert len(result) == 1
        assert result.iloc[0]["plant_name"] == "Updated"

