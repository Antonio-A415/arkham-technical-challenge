
from unittest.mock import MagicMock, patch

from connector.extractor import _make_snapshot_id, _map_generator_record, _map_facility_record, _map_us_record



# ---------------------------------------------------------------------------
# Extractor mapping tests
# ---------------------------------------------------------------------------

class TestExtractorMapping:
    # Exact record shape from real EIA API response (2026-03-25)
    GENERATOR_RAW = {
        "period": "2026-03-25",
        "facility": "46",
        "facilityName": "Browns Ferry",
        "generator": "1",
        "capacity": 1254.8,
        "outage": 0.0,
        "percentOutage": 0.0,
        "capacity-units": "megawatts",
        "outage-units": "megawatts",
        "percentOutage-units": "percent",
    }
    FACILITY_RAW = {
        "period": "2026-03-25",
        "facility": "46",
        "facilityName": "Browns Ferry",
        "capacity": 3763.4,
        "outage": 1259.0,
        "percentOutage": 33.4,
    }

    def test_generator_map_level(self):
        row = _map_generator_record(self.GENERATOR_RAW)
        assert row["level"] == "generator"
        assert row["plant_id"] == "46"
        assert row["plant_name"] == "Browns Ferry"
        assert row["generator_id"] == "1"
        assert row["capacity_mw"] == 1254.8

    def test_facility_map_level(self):
        row = _map_facility_record(self.FACILITY_RAW)
        assert row["level"] == "facility"
        assert row["generator_id"] is None
        assert row["plant_id"] == "46"

    def test_snapshot_id_is_deterministic(self):
        id1 = _make_snapshot_id("46", "1", "2026-03-25", "generator")
        id2 = _make_snapshot_id("46", "1", "2026-03-25", "generator")
        assert id1 == id2

    def test_snapshot_id_differs_by_level(self):
        id_gen = _make_snapshot_id("46", "1", "2026-03-25", "generator")
        id_fac = _make_snapshot_id("46", None, "2026-03-25", "facility")
        assert id_gen != id_fac

    def test_snapshot_id_differs_by_generator(self):
        id1 = _make_snapshot_id("46", "1", "2026-03-25", "generator")
        id2 = _make_snapshot_id("46", "2", "2026-03-25", "generator")
        assert id1 != id2

    def test_us_record_mapped_correctly(self):
        raw = {
            "period": "2026-03-25",
            "capacity": 95432.1,
            "outage": 4812.0,
            "percentOutage": 5.04,
        }
        row = _map_us_record(raw)
        assert row["report_date"] == "2026-03-25"
        assert row["total_capacity_mw"] == 95432.1
        assert row["total_outage_mw"] == 4812.0
        assert row["percent_outage"] == 5.04

    def test_us_record_no_facility_keys(self):
        raw = {"period": "2026-03-25", "capacity": 100.0, "outage": 5.0, "percentOutage": 5.0}
        row = _map_us_record(raw)
        assert "plant_id" not in row
        assert "generator_id" not in row

