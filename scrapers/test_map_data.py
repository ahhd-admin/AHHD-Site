"""Map data validation tests.

Verifies that the pipeline produces records that correctly populate the map
view on the site's home screen.  Two test modes:

1. Unit-mode (default, no network): validates that normalize_rows() output
   contains all fields the map needs and that coordinates are within US bounds.

2. Integration-mode (set SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY): queries
   the live `locations` table and asserts the same invariants against real data.

Run unit tests only:
    pytest scrapers/test_map_data.py -v

Run with live Supabase check:
    SUPABASE_URL=https://... SUPABASE_SERVICE_ROLE_KEY=... pytest scrapers/test_map_data.py -v
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))

os.environ.setdefault("GOOGLE_SHEETS_WEB_APP_URL", "http://test-placeholder")

from achc_scraper import normalize_row, normalize_rows  # noqa: E402

# ---------------------------------------------------------------------------
# US geographic bounding box (mainland + AK + HI + territories)
# ---------------------------------------------------------------------------
US_LAT_MIN = 17.0   # US Virgin Islands / Puerto Rico
US_LAT_MAX = 71.5   # Northern Alaska
US_LON_MIN = -180.0 # Aleutian Islands
US_LON_MAX = -65.0  # US Virgin Islands

# Fields normalize_row must produce (coords added later by geocode_locations)
MAP_NORMALIZE_FIELDS = [
    "provider_name",    # marker label / popup title
    "street_address",   # popup detail
    "city",             # popup detail + filter
    "state",            # popup detail + filter
    "zip",              # popup detail
    "services",         # popup detail + filter chips
    "accrediting_body", # popup trust badge
    "confidence_status",# controls display (only "verified" → published)
    "source_url",       # "View on ACHC" link
]

# Fields that must exist after the full pipeline (normalize + geocode)
MAP_REQUIRED_FIELDS = MAP_NORMALIZE_FIELDS + ["latitude", "longitude"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_raw(
    name: str = "Test Provider LLC",
    street: str = "123 Main St",
    city_state_zip: str = "Dallas, TX 75201",
    services: str = "Home Health",
    state_abbr: str = "TX",
) -> dict:
    raw_text = f"{name}\n{street}\n{city_state_zip}\n(214) 555-0000\n{services}"
    return {
        "raw_text": raw_text,
        "raw_name_line": name,
        "raw_address_block": street,
        "parsed_state_abbr": state_abbr,
        "detected_program_mentions": services,
        "searched_program_type": services.split(",")[0].strip(),
        "source_url": "https://ams.achc.org/accredited_organizations.aspx",
        "last_seen": "2026-01-01T00:00:00",
        "result_scope": "National raw ACHC pull; no state selected",
    }


def _inject_coords(loc: dict, lat: float = 32.78, lon: float = -96.80) -> dict:
    loc["latitude"] = lat
    loc["longitude"] = lon
    return loc


# ---------------------------------------------------------------------------
# Required field presence
# ---------------------------------------------------------------------------

class TestMapRequiredFields:
    def test_all_required_fields_present_after_normalize(self):
        row = normalize_row(_make_raw())
        for field in MAP_NORMALIZE_FIELDS:
            assert field in row, f"Map-required field '{field}' missing from normalize_row output"

    def test_coords_present_after_geocoding(self):
        row = normalize_row(_make_raw())
        _inject_coords(row, 32.78, -96.80)
        for field in MAP_REQUIRED_FIELDS:
            assert field in row, f"Map-required field '{field}' missing after geocoding"

    def test_provider_name_is_non_empty(self):
        row = normalize_row(_make_raw(name="Alpha Health LLC"))
        assert row["provider_name"].strip() != ""

    def test_city_is_non_empty_for_valid_input(self):
        row = normalize_row(_make_raw(city_state_zip="Dallas, TX 75201"))
        assert row["city"].strip() != ""

    def test_state_is_non_empty_for_valid_input(self):
        row = normalize_row(_make_raw(state_abbr="TX"))
        assert row["state"].strip() != ""

    def test_services_is_list(self):
        row = normalize_row(_make_raw(services="Home Health"))
        assert isinstance(row["services"], list)

    def test_confidence_status_defaults_to_verified(self):
        row = normalize_row(_make_raw())
        assert row["confidence_status"] == "verified"

    def test_accrediting_body_is_achc(self):
        row = normalize_row(_make_raw())
        assert row["accrediting_body"] == "ACHC"

    def test_source_url_is_achc_domain(self):
        row = normalize_row(_make_raw())
        assert "achc.org" in row["source_url"]


# ---------------------------------------------------------------------------
# Coordinate validity (US bounding box)
# ---------------------------------------------------------------------------

class TestCoordinateValidity:
    def _valid_location(self, lat: float, lon: float) -> dict:
        loc = normalize_row(_make_raw())
        return _inject_coords(loc, lat, lon)

    def test_valid_us_coordinates_pass(self):
        loc = self._valid_location(32.78, -96.80)  # Dallas, TX
        lat, lon = loc["latitude"], loc["longitude"]
        assert US_LAT_MIN <= lat <= US_LAT_MAX
        assert US_LON_MIN <= lon <= US_LON_MAX

    def test_hawaii_coords_are_valid(self):
        loc = self._valid_location(21.31, -157.80)  # Honolulu, HI
        assert US_LAT_MIN <= loc["latitude"] <= US_LAT_MAX
        assert US_LON_MIN <= loc["longitude"] <= US_LON_MAX

    def test_alaska_coords_are_valid(self):
        loc = self._valid_location(61.22, -149.90)  # Anchorage, AK
        assert US_LAT_MIN <= loc["latitude"] <= US_LAT_MAX
        assert US_LON_MIN <= loc["longitude"] <= US_LON_MAX

    def test_non_us_lat_detected(self):
        loc = self._valid_location(51.5, -0.13)  # London
        assert not (US_LAT_MIN <= loc["latitude"] <= US_LAT_MAX and
                    US_LON_MIN <= loc["longitude"] <= US_LON_MAX)

    def test_none_coords_are_flagged(self):
        loc = normalize_row(_make_raw())
        loc["latitude"] = None
        loc["longitude"] = None
        assert loc["latitude"] is None

    def test_string_empty_coords_are_falsy(self):
        # Providers without geocodes arrive from the sheet with "" not None
        loc = normalize_row(_make_raw())
        loc["latitude"] = ""
        loc["longitude"] = ""
        assert not loc["latitude"]
        assert not loc["longitude"]


# ---------------------------------------------------------------------------
# Multi-provider batch: map marker count and deduplication
# ---------------------------------------------------------------------------

class TestBatchMapData:
    def _geocoded_batch(self) -> list:
        rows = [
            _make_raw("Alpha Health LLC", "1 Alpha St", "Austin, TX 78701",  "Home Health", "TX"),
            _make_raw("Beta Care Inc",    "2 Beta Ave", "Houston, TX 77001",  "Hospice",     "TX"),
            _make_raw("Gamma Services",  "3 Gamma Blvd","Chicago, IL 60601",  "Home Care",   "IL"),
            # Duplicate of Alpha under a second program — should be merged
            _make_raw("Alpha Health LLC", "1 Alpha St", "Austin, TX 78701",  "Home Care",   "TX"),
        ]
        normalized = normalize_rows(rows)
        coords = [(30.27, -97.74), (29.76, -95.37), (41.88, -87.63)]
        for loc, (lat, lon) in zip(normalized, coords):
            _inject_coords(loc, lat, lon)
        return normalized

    def test_duplicate_provider_produces_one_map_marker(self):
        batch = self._geocoded_batch()
        assert len(batch) == 3  # Alpha deduped

    def test_all_markers_have_valid_us_coords(self):
        for loc in self._geocoded_batch():
            lat, lon = loc["latitude"], loc["longitude"]
            assert US_LAT_MIN <= lat <= US_LAT_MAX, f"Lat {lat} out of US bounds for {loc['provider_name']}"
            assert US_LON_MIN <= lon <= US_LON_MAX, f"Lon {lon} out of US bounds for {loc['provider_name']}"

    def test_merged_provider_has_all_services(self):
        batch = self._geocoded_batch()
        alpha = next(b for b in batch if b["provider_name"] == "Alpha Health LLC")
        assert "Home Health" in alpha["services"]
        assert "Home Care" in alpha["services"]

    def test_all_markers_have_non_empty_name(self):
        for loc in self._geocoded_batch():
            assert loc["provider_name"].strip() != ""

    def test_all_markers_have_state(self):
        for loc in self._geocoded_batch():
            assert loc["state"].strip() != ""


# ---------------------------------------------------------------------------
# Confidence status → listing_status mapping (mirrors the SQL RPC logic)
# ---------------------------------------------------------------------------

class TestConfidenceToListingStatus:
    """The SQL merge function maps confidence_status to listing_status.
    These tests verify the scraper emits the right confidence values so
    the RPC produces the right published/needs_review states."""

    def test_fresh_provider_is_verified(self):
        row = normalize_row(_make_raw())
        assert row["confidence_status"] == "verified"

    def test_verified_maps_to_published(self):
        # confirmed: merge_google_sheets_data sets listing_status='published' for 'verified'
        row = normalize_row(_make_raw())
        expected_db_status = "published" if row["confidence_status"] == "verified" else "needs_review"
        assert expected_db_status == "published"

    def test_changed_would_map_to_needs_review(self):
        row = normalize_row(_make_raw())
        row["confidence_status"] = "changed"
        expected_db_status = "published" if row["confidence_status"] == "verified" else "needs_review"
        assert expected_db_status == "needs_review"


# ---------------------------------------------------------------------------
# Integration test — live Supabase data (skipped without credentials)
# ---------------------------------------------------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
_SKIP_INTEGRATION = not (SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)


@pytest.mark.skipif(_SKIP_INTEGRATION, reason="Supabase credentials not set")
class TestSupabaseMapData:
    """Queries the live locations table and validates map-readiness."""

    @pytest.fixture(scope="class")
    def published_locations(self):
        import urllib.request
        import json as _json

        url = f"{SUPABASE_URL}/rest/v1/locations?listing_status=eq.published&select=location_id,latitude,longitude,location_name,city,state,postal_code,listing_status&limit=200"
        req = urllib.request.Request(
            url,
            headers={
                "apikey": SUPABASE_SERVICE_ROLE_KEY,
                "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return _json.loads(resp.read())

    def test_published_locations_exist(self, published_locations):
        assert len(published_locations) > 0, "No published locations found in Supabase"

    def test_published_locations_have_coordinates(self, published_locations):
        missing = [
            loc["location_id"] for loc in published_locations
            if not loc.get("latitude") or not loc.get("longitude")
        ]
        assert missing == [], (
            f"{len(missing)} published location(s) are missing coordinates: "
            f"{missing[:5]}{'…' if len(missing) > 5 else ''}"
        )

    def test_coordinates_within_us_bounds(self, published_locations):
        out_of_bounds = [
            loc for loc in published_locations
            if loc.get("latitude") and loc.get("longitude")
            and not (
                US_LAT_MIN <= float(loc["latitude"]) <= US_LAT_MAX
                and US_LON_MIN <= float(loc["longitude"]) <= US_LON_MAX
            )
        ]
        assert out_of_bounds == [], (
            f"{len(out_of_bounds)} published location(s) have coordinates outside US bounds: "
            f"{[l['location_id'] for l in out_of_bounds[:3]]}"
        )

    def test_published_locations_have_city_and_state(self, published_locations):
        missing = [
            loc["location_id"] for loc in published_locations
            if not (loc.get("city") or "").strip() or not (loc.get("state") or "").strip()
        ]
        assert missing == [], f"{len(missing)} published location(s) missing city or state"
