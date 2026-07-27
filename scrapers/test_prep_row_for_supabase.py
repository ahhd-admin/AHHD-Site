"""Regression tests for prep_row_for_supabase() in achc_scraper.py.

Guards against the field-remap bug where street_address/provider_name were
renamed to address/organization before being sent to the merge_google_sheets_data
RPC — but that RPC reads row_data->>'street_address' and
row_data->>'provider_name' directly (see
supabase/migrations/20260101000000_merge_google_sheets_data.sql), so the
renamed fields were silently dropped and address_line_1 landed NULL on every
direct-to-Supabase write.

Run with:  pytest scrapers/test_prep_row_for_supabase.py -v
"""

import os
import sys

# achc_scraper raises ValueError at import time if GOOGLE_SHEETS_WEB_APP_URL
# is not set and ENABLE_DIRECT_SUPABASE is disabled. Provide a placeholder so
# the module can be imported for testing regardless of env state.
os.environ.setdefault("GOOGLE_SHEETS_WEB_APP_URL", "http://test-placeholder")

sys.path.insert(0, os.path.dirname(__file__))

from achc_scraper import prep_row_for_supabase  # noqa: E402


def _row(**overrides) -> dict:
    base = {
        "provider_name": "Helping Hands Home Health",
        "street_address": "123 Main St",
        "city": "Dallas",
        "state": "TX",
        "zip": "75201",
        "services": ["Home Health", "Hospice"],
        "latitude": 32.78,
        "longitude": -96.80,
    }
    base.update(overrides)
    return base


class TestPrepRowForSupabase:
    def test_provider_name_passes_through_unchanged(self):
        r = prep_row_for_supabase(_row())
        assert r["provider_name"] == "Helping Hands Home Health"
        assert "organization" not in r

    def test_street_address_passes_through_unchanged(self):
        r = prep_row_for_supabase(_row())
        assert r["street_address"] == "123 Main St"
        assert "address" not in r

    def test_services_list_serialized_to_comma_string(self):
        r = prep_row_for_supabase(_row())
        assert r["services"] == "Home Health,Hospice"

    def test_services_string_left_as_is(self):
        r = prep_row_for_supabase(_row(services="Home Health,Hospice"))
        assert r["services"] == "Home Health,Hospice"

    def test_geocode_status_ok_when_latitude_present(self):
        r = prep_row_for_supabase(_row(latitude=32.78))
        assert r["geocode_status"] == "ok"

    def test_geocode_status_pending_when_no_latitude(self):
        r = prep_row_for_supabase(_row(latitude=None, longitude=None))
        assert r["geocode_status"] == "pending"

    def test_existing_geocode_status_not_overwritten(self):
        r = prep_row_for_supabase(_row(latitude=None, geocode_status="failed"))
        assert r["geocode_status"] == "failed"

    def test_does_not_mutate_input_row(self):
        original = _row()
        prep_row_for_supabase(original)
        assert original["services"] == ["Home Health", "Hospice"]
        assert "geocode_status" not in original
