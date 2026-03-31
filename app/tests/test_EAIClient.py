"""
Unit tests for the nuclear outages pipeline.
Run with: pytest tests/ -v
"""



# ---------------------------------------------------------------------------
# EIAClient auth tests
# ---------------------------------------------------------------------------

import pytest
from pathlib import Path


class TestEIAClient:
    def test_raises_auth_error_when_no_key(self, monkeypatch):
        from connector.eia_client import EIAClient, EIAAuthError
        monkeypatch.delenv("API_KEY", raising=False)
        monkeypatch.delenv("EIA_BASE_URL", raising=False)
        with pytest.raises(EIAAuthError):
            EIAClient(api_key=None)

    def test_accepts_explicit_key(self):
        from connector.eia_client import EIAClient
        client = EIAClient(api_key="test_key_123")
        assert client.api_key == "test_key_123"

    def test_base_url_from_env(self, monkeypatch):
        from connector.eia_client import EIAClient
        monkeypatch.setenv("EIA_BASE_URL", "http://localhost:9999/v2")
        client = EIAClient(api_key="test_key_123")
        assert client.base_url == "http://localhost:9999/v2"

    def test_base_url_explicit_arg_wins_over_env(self, monkeypatch):
        from connector.eia_client import EIAClient
        monkeypatch.setenv("EIA_BASE_URL", "http://env-url/v2")
        client = EIAClient(api_key="k", base_url="http://explicit/v2")
        assert client.base_url == "http://explicit/v2"

    def test_base_url_strips_trailing_slash(self):
        from connector.eia_client import EIAClient
        client = EIAClient(api_key="k", base_url="https://api.eia.gov/v2/")
        assert not client.base_url.endswith("/")

    def test_default_base_url(self, monkeypatch):
        from connector.eia_client import EIAClient
        monkeypatch.delenv("EIA_BASE_URL", raising=False)
        client = EIAClient(api_key="k")
        assert client.base_url == "https://api.eia.gov/v2"

    def test_three_route_constants_defined(self):
        from connector.eia_client import (
            ROUTE_GENERATOR_OUTAGES,
            ROUTE_FACILITY_OUTAGES,
            ROUTE_US_OUTAGES,
        )
        assert "generator" in ROUTE_GENERATOR_OUTAGES
        assert "facility"  in ROUTE_FACILITY_OUTAGES
        assert "us"        in ROUTE_US_OUTAGES