
from connector.validator import validate_record, validate_batch


# ---------------------------------------------------------------------------
# Validator tests
# ---------------------------------------------------------------------------

class TestValidator:
    def test_valid_record_passes(self):
        record = {
            "period": "2026-03-25",
            "facility": "46",
            "facilityName": "Browns Ferry",
            "generator": "1",
            "capacity": 1254.8,
            "outage": 0.0,
            "percentOutage": 0.0,
        }
        result = validate_record(record)
        assert result is not None
        assert result["capacity"] == 1254.8

    def test_missing_required_field_drops_record(self):
        record = {
            "period": "2026-03-25",
            # Missing facility and facilityName
        }
        result = validate_record(record)
        assert result is None

    def test_numeric_coercion_handles_none(self):
        record = {
            "period": "2026-03-25",
            "facility": "46",
            "facilityName": "Browns Ferry",
            "capacity": None,
            "outage": "",
        }
        result = validate_record(record)
        assert result is not None
        assert result["capacity"] is None
        assert result["outage"] is None

    def test_batch_returns_counts(self):
        records = [
            {"period": "2026-03-25", "facility": "46", "facilityName": "Browns Ferry"},
            {"period": "", "facility": "", "facilityName": ""},  # invalid
            {"period": "2026-03-25", "facility": "204", "facilityName": "Clinton"},
        ]
        valid, dropped = validate_batch(records)
        assert len(valid) == 2
        assert dropped == 1
