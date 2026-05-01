"""Unit tests for normalize_row() and normalize_rows() in achc_scraper.py.

Run with:  pytest scrapers/test_normalize.py -v
"""

import os
import sys

# achc_scraper raises ValueError at import time if GOOGLE_SHEETS_WEB_APP_URL
# is not set.  Provide a placeholder so the module can be imported for testing.
os.environ.setdefault("GOOGLE_SHEETS_WEB_APP_URL", "http://test-placeholder")

# Allow direct import from the same directory without a package install.
sys.path.insert(0, os.path.dirname(__file__))

from achc_scraper import normalize_row, normalize_rows  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_raw_row(
    raw_text: str,
    raw_name_line: str = "",
    raw_address_block: str = "",
    parsed_state_abbr: str = "",
    detected_program_mentions: str = "",
    searched_program_type: str = "",
) -> dict:
    """Build a minimal raw-row dict that matches what scrape_raw_rows() produces."""
    return {
        "raw_text": raw_text,
        "raw_name_line": raw_name_line,
        "raw_address_block": raw_address_block,
        "parsed_state_abbr": parsed_state_abbr,
        "detected_program_mentions": detected_program_mentions,
        "searched_program_type": searched_program_type,
        "source_url": "https://ams.achc.org/accredited_organizations.aspx",
        "last_seen": "2026-01-01T00:00:00",
        "result_scope": "National raw ACHC pull; no state selected",
    }


# A realistic raw_text for a Texas provider.
TYPICAL_RAW_TEXT = (
    "Provider Name LLC\n"
    "123 Main St\n"
    "Dallas, TX 75201\n"
    "(214) 555-1234\n"
    "Home Health | Home Care"
)

TYPICAL_ROW = make_raw_row(
    raw_text=TYPICAL_RAW_TEXT,
    raw_name_line="Provider Name LLC",
    raw_address_block="123 Main St",
    parsed_state_abbr="TX",
    detected_program_mentions="Home Care, Home Health",
    searched_program_type="Home Health",
)


# ---------------------------------------------------------------------------
# normalize_row() — city extraction
# ---------------------------------------------------------------------------

class TestNormalizeRowCity:
    def test_parses_city_from_raw_text(self):
        result = normalize_row(TYPICAL_ROW)
        assert result["city"] == "Dallas"

    def test_city_with_multi_word_name(self):
        raw_text = (
            "Another Provider Inc\n"
            "456 Oak Ave\n"
            "San Antonio, TX 78201\n"
            "(210) 555-9876"
        )
        row = make_raw_row(
            raw_text=raw_text,
            raw_name_line="Another Provider Inc",
            raw_address_block="456 Oak Ave",
            parsed_state_abbr="TX",
        )
        result = normalize_row(row)
        assert result["city"] == "San Antonio"

    def test_city_empty_when_raw_text_blank(self):
        row = make_raw_row(
            raw_text="",
            raw_name_line="Some Provider",
            raw_address_block="",
            parsed_state_abbr="",
        )
        result = normalize_row(row)
        assert result["city"] == ""

    def test_city_not_extracted_when_line_starts_with_digit(self):
        # A line like "123 Broad St, TX 75201" should NOT be parsed as city.
        raw_text = "Provider LLC\n123 Broad St, TX 75201\n(214) 555-0000"
        row = make_raw_row(
            raw_text=raw_text,
            raw_name_line="Provider LLC",
            raw_address_block="",
            parsed_state_abbr="TX",
        )
        result = normalize_row(row)
        assert result["city"] == ""


# ---------------------------------------------------------------------------
# normalize_row() — zip extraction
# ---------------------------------------------------------------------------

class TestNormalizeRowZip:
    def test_parses_zip_5digit(self):
        result = normalize_row(TYPICAL_ROW)
        assert result["zip"] == "75201"

    def test_parses_zip_plus4(self):
        raw_text = (
            "Health Corp\n"
            "789 Elm Blvd\n"
            "Houston, TX 77001-2345\n"
            "(713) 555-4321"
        )
        row = make_raw_row(
            raw_text=raw_text,
            raw_name_line="Health Corp",
            raw_address_block="789 Elm Blvd",
            parsed_state_abbr="TX",
        )
        result = normalize_row(row)
        assert result["zip"] == "77001-2345"

    def test_zip_empty_when_no_city_state_zip_line(self):
        row = make_raw_row(
            raw_text="Provider LLC\nSome text with no address",
            raw_name_line="Provider LLC",
            raw_address_block="",
            parsed_state_abbr="",
        )
        result = normalize_row(row)
        assert result["zip"] == ""


# ---------------------------------------------------------------------------
# normalize_row() — street_address extraction
# ---------------------------------------------------------------------------

class TestNormalizeRowStreetAddress:
    def test_street_address_plain(self):
        result = normalize_row(TYPICAL_ROW)
        assert result["street_address"] == "123 Main St"

    def test_street_address_no_city_state_zip(self):
        """street_address must NOT contain city, state, or ZIP."""
        result = normalize_row(TYPICAL_ROW)
        addr = result["street_address"]
        assert "Dallas" not in addr
        assert "TX" not in addr
        assert "75201" not in addr

    def test_street_address_splits_pipe_takes_first(self):
        """raw_address_block with ' | ' separator — take only the first segment."""
        row = make_raw_row(
            raw_text=TYPICAL_RAW_TEXT,
            raw_name_line="Provider Name LLC",
            raw_address_block="123 Main St | Suite 200",
            parsed_state_abbr="TX",
            detected_program_mentions="Home Health",
        )
        result = normalize_row(row)
        assert result["street_address"] == "123 Main St"

    def test_street_address_empty_when_no_address_block(self):
        row = make_raw_row(
            raw_text=TYPICAL_RAW_TEXT,
            raw_name_line="Provider Name LLC",
            raw_address_block="",
            parsed_state_abbr="TX",
        )
        result = normalize_row(row)
        assert result["street_address"] == ""


# ---------------------------------------------------------------------------
# normalize_row() — provider_key slug
# ---------------------------------------------------------------------------

class TestNormalizeRowProviderKey:
    def test_provider_key_format(self):
        result = normalize_row(TYPICAL_ROW)
        # Expected: slug of name + "_" + state abbr
        assert result["provider_key"] == "provider-name-llc_TX"

    def test_provider_key_no_state(self):
        row = make_raw_row(
            raw_text=TYPICAL_RAW_TEXT,
            raw_name_line="Provider Name LLC",
            raw_address_block="123 Main St",
            parsed_state_abbr="",
        )
        result = normalize_row(row)
        assert result["provider_key"] == "provider-name-llc"

    def test_provider_key_special_chars_stripped(self):
        row = make_raw_row(
            raw_text=TYPICAL_RAW_TEXT,
            raw_name_line="ABC & XYZ, Inc.",
            raw_address_block="1 Test Rd",
            parsed_state_abbr="TX",
        )
        result = normalize_row(row)
        # safe_slug replaces non-alphanumeric runs with "-" and strips leading/trailing "-"
        assert result["provider_key"] == "abc-xyz-inc_TX"


# ---------------------------------------------------------------------------
# normalize_row() — missing / empty field resilience
# ---------------------------------------------------------------------------

class TestNormalizeRowMissingFields:
    def test_empty_dict_does_not_crash(self):
        result = normalize_row({})
        assert isinstance(result, dict)

    def test_all_string_fields_are_strings(self):
        result = normalize_row({})
        for field in ("provider_key", "provider_name", "dba_name", "street_address",
                      "city", "state", "zip", "phone", "website",
                      "accrediting_body", "source_url", "last_seen",
                      "confidence_status", "searched_program_type", "result_scope"):
            assert isinstance(result[field], str), f"Field '{field}' is not a string"

    def test_services_is_list(self):
        result = normalize_row({})
        assert isinstance(result["services"], list)

    def test_none_values_do_not_crash(self):
        row = {
            "raw_text": None,
            "raw_name_line": None,
            "raw_address_block": None,
            "parsed_state_abbr": None,
            "detected_program_mentions": None,
        }
        result = normalize_row(row)
        assert isinstance(result, dict)
        assert result["city"] == ""
        assert result["zip"] == ""

    def test_new_fields_present(self):
        """website and dba_name are always present and default to empty string."""
        result = normalize_row(TYPICAL_ROW)
        assert "website" in result
        assert result["website"] == ""
        assert "dba_name" in result
        assert result["dba_name"] == ""


# ---------------------------------------------------------------------------
# normalize_rows() — deduplication
# ---------------------------------------------------------------------------

class TestNormalizeRowsDeduplication:
    def _make_home_health_row(self):
        return make_raw_row(
            raw_text=TYPICAL_RAW_TEXT,
            raw_name_line="Provider Name LLC",
            raw_address_block="123 Main St",
            parsed_state_abbr="TX",
            detected_program_mentions="Home Health",
            searched_program_type="Home Health",
        )

    def _make_home_care_row(self):
        raw_text = (
            "Provider Name LLC\n"
            "123 Main St\n"
            "Dallas, TX 75201\n"
            "(214) 555-1234\n"
            "Home Care"
        )
        return make_raw_row(
            raw_text=raw_text,
            raw_name_line="Provider Name LLC",
            raw_address_block="123 Main St",
            parsed_state_abbr="TX",
            detected_program_mentions="Home Care",
            searched_program_type="Home Care",
        )

    def test_two_rows_same_key_become_one(self):
        raw_rows = [self._make_home_health_row(), self._make_home_care_row()]
        result = normalize_rows(raw_rows)
        assert len(result) == 1

    def test_services_merged_from_both_rows(self):
        raw_rows = [self._make_home_health_row(), self._make_home_care_row()]
        result = normalize_rows(raw_rows)
        services = result[0]["services"]
        assert "Home Care" in services
        assert "Home Health" in services

    def test_services_deduplicated_when_same_in_both(self):
        raw_rows = [self._make_home_health_row(), self._make_home_health_row()]
        result = normalize_rows(raw_rows)
        assert len(result) == 1
        assert result[0]["services"].count("Home Health") == 1

    def test_non_service_fields_from_first_occurrence(self):
        """Non-service fields (phone, city, etc.) come from the first occurrence."""
        raw_rows = [self._make_home_health_row(), self._make_home_care_row()]
        result = normalize_rows(raw_rows)
        # searched_program_type from first row
        assert result[0]["searched_program_type"] == "Home Health"

    def test_distinct_providers_both_kept(self):
        row_a = make_raw_row(
            raw_text=TYPICAL_RAW_TEXT,
            raw_name_line="Alpha Health LLC",
            raw_address_block="1 Alpha Rd",
            parsed_state_abbr="TX",
            detected_program_mentions="Home Health",
        )
        row_b = make_raw_row(
            raw_text=(
                "Beta Care Inc\n"
                "2 Beta Ave\n"
                "Austin, TX 78701\n"
                "(512) 555-0000"
            ),
            raw_name_line="Beta Care Inc",
            raw_address_block="2 Beta Ave",
            parsed_state_abbr="TX",
            detected_program_mentions="Home Care",
        )
        result = normalize_rows([row_a, row_b])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# normalize_rows() — order preservation
# ---------------------------------------------------------------------------

class TestNormalizeRowsOrder:
    def test_preserves_first_occurrence_order(self):
        names = ["Zeta Provider LLC", "Alpha Provider LLC", "Mango Health Inc"]
        raw_rows = [
            make_raw_row(
                raw_text=f"{name}\n100 Test St\nDallas, TX 75201\n(214) 555-0000",
                raw_name_line=name,
                raw_address_block="100 Test St",
                parsed_state_abbr="TX",
                detected_program_mentions="Home Health",
            )
            for name in names
        ]
        result = normalize_rows(raw_rows)
        assert [r["provider_name"] for r in result] == names

    def test_duplicate_appended_later_does_not_change_position(self):
        """The duplicate row must NOT move the first-occurrence entry."""
        row1 = make_raw_row(
            raw_text=TYPICAL_RAW_TEXT,
            raw_name_line="Provider Name LLC",
            raw_address_block="123 Main St",
            parsed_state_abbr="TX",
            detected_program_mentions="Home Health",
        )
        row2 = make_raw_row(
            raw_text=(
                "Other Provider Inc\n"
                "999 Other St\n"
                "Austin, TX 78701\n"
                "(512) 555-9999"
            ),
            raw_name_line="Other Provider Inc",
            raw_address_block="999 Other St",
            parsed_state_abbr="TX",
            detected_program_mentions="Hospice",
        )
        # Duplicate of row1 placed at the end
        row1_dup = make_raw_row(
            raw_text=TYPICAL_RAW_TEXT,
            raw_name_line="Provider Name LLC",
            raw_address_block="123 Main St",
            parsed_state_abbr="TX",
            detected_program_mentions="Home Care",
        )
        result = normalize_rows([row1, row2, row1_dup])
        assert len(result) == 2
        # First entry is still Provider Name LLC (position 0)
        assert result[0]["provider_name"] == "Provider Name LLC"
        assert result[1]["provider_name"] == "Other Provider Inc"
