"""Unit tests for geocode_helper.py.

All network calls are mocked — no real API requests are made.

Run with:  pytest scrapers/test_geocode_helper.py -v
"""

import asyncio
import json
import os
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(__file__))

os.environ.setdefault("GOOGLE_MAPS_API_KEY", "TEST_KEY")
os.environ.setdefault("GEOCODE_CONCURRENCY", "5")

import geocode_helper  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_geocode_response(lat: float, lng: float, status: str = "OK") -> dict:
    return {
        "status": status,
        "results": [
            {"geometry": {"location": {"lat": lat, "lng": lng}}}
        ],
    }


def _make_empty_response(status: str = "ZERO_RESULTS") -> dict:
    return {"status": status, "results": []}


def _provider(
    street_address: str = "123 Main St",
    city: str = "Dallas",
    state: str = "TX",
    zip_code: str = "75201",
) -> dict:
    return {
        "street_address": street_address,
        "city": city,
        "state": state,
        "zip": zip_code,
        "provider_name": "Test Provider LLC",
    }


# ---------------------------------------------------------------------------
# address_key
# ---------------------------------------------------------------------------

class TestAddressKey:
    def test_basic(self):
        k = geocode_helper.address_key("123 Main St", "Dallas", "TX", "75201")
        assert k == "123 main st|dallas|TX|75201"

    def test_case_normalization(self):
        k1 = geocode_helper.address_key("123 MAIN ST", "DALLAS", "tx", "75201")
        k2 = geocode_helper.address_key("123 main st", "dallas", "TX", "75201")
        assert k1 == k2

    def test_strips_whitespace(self):
        k = geocode_helper.address_key(" 123 Main St ", " Dallas ", " TX ", " 75201 ")
        assert k == "123 main st|dallas|TX|75201"

    def test_zip_coerced_to_string(self):
        k = geocode_helper.address_key("1 A St", "City", "TX", 75201)
        assert k.endswith("|75201")


# ---------------------------------------------------------------------------
# geocode_address — mocked aiohttp
# ---------------------------------------------------------------------------

class TestGeocodeAddress:
    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_session(self, response_json: dict, status: int = 200):  # noqa: E501
        resp = AsyncMock()
        resp.status = status
        resp.json = AsyncMock(return_value=response_json)
        resp.__aenter__ = AsyncMock(return_value=resp)
        resp.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.get = MagicMock(return_value=resp)
        return session

    def test_returns_lat_lon_on_success(self):
        session = self._mock_session(_make_geocode_response(32.78, -96.80))
        sem = asyncio.Semaphore(1)
        lat, lon = self._run(geocode_helper.geocode_address(session, sem, "123 Main St", "Dallas", "TX", "75201"))
        assert lat == pytest.approx(32.78)
        assert lon == pytest.approx(-96.80)

    def test_returns_none_on_zero_results(self):
        session = self._mock_session(_make_empty_response("ZERO_RESULTS"))
        sem = asyncio.Semaphore(1)
        lat, lon = self._run(geocode_helper.geocode_address(session, sem, "Bad Address", "Nowhere", "TX", "00000"))
        assert lat is None
        assert lon is None

    def test_returns_none_on_http_error(self):
        session = self._mock_session({}, status=500)
        sem = asyncio.Semaphore(1)
        lat, lon = self._run(geocode_helper.geocode_address(session, sem, "123 Main St", "Dallas", "TX", "75201"))
        assert lat is None
        assert lon is None

    def test_returns_none_on_network_exception(self):
        resp = AsyncMock()
        resp.__aenter__ = AsyncMock(side_effect=Exception("timeout"))
        resp.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.get = MagicMock(return_value=resp)
        sem = asyncio.Semaphore(1)
        lat, lon = self._run(geocode_helper.geocode_address(session, sem, "123 Main St", "Dallas", "TX", "75201"))
        assert lat is None
        assert lon is None


# ---------------------------------------------------------------------------
# geocode_locations — full pipeline with mocks
# ---------------------------------------------------------------------------

_FAKE_API_RESPONSE = _make_geocode_response(32.78, -96.80)


class TestGeocodeLocations:
    def _run(self, coro):
        return asyncio.run(coro)

    def test_skips_geocoding_when_no_api_key(self, tmp_path, monkeypatch):
        monkeypatch.setattr(geocode_helper, "GOOGLE_MAPS_API_KEY", "")
        locs = [_provider()]
        result = self._run(geocode_helper.geocode_locations(locs))
        assert result[0].get("latitude") is None

    def test_uses_cache_when_available(self, tmp_path, monkeypatch):
        key = geocode_helper.address_key("123 Main St", "Dallas", "TX", "75201")
        cache = {key: {"latitude": 32.78, "longitude": -96.80}}
        cache_file = tmp_path / "geocode_cache.json"
        cache_file.write_text(json.dumps(cache))

        monkeypatch.setattr(geocode_helper, "CACHE_FILE", str(cache_file))
        monkeypatch.setattr(geocode_helper, "GOOGLE_MAPS_API_KEY", "TEST_KEY")

        locs = [_provider()]

        # geocode_address should never be called (cache hit)
        with patch.object(geocode_helper, "geocode_address", new_callable=AsyncMock) as mock_ga:
            self._run(geocode_helper.geocode_locations(locs))
            mock_ga.assert_not_called()

        assert locs[0]["latitude"] == pytest.approx(32.78)
        assert locs[0]["longitude"] == pytest.approx(-96.80)

    def test_geocodes_uncached_addresses(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "geocode_cache.json"
        monkeypatch.setattr(geocode_helper, "CACHE_FILE", str(cache_file))
        monkeypatch.setattr(geocode_helper, "GOOGLE_MAPS_API_KEY", "TEST_KEY")

        locs = [_provider()]

        with patch.object(geocode_helper, "geocode_address", new_callable=AsyncMock) as mock_ga:
            mock_ga.return_value = (32.78, -96.80)
            self._run(geocode_helper.geocode_locations(locs))

        assert locs[0]["latitude"] == pytest.approx(32.78)
        assert locs[0]["longitude"] == pytest.approx(-96.80)

    def test_result_written_to_cache(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "geocode_cache.json"
        monkeypatch.setattr(geocode_helper, "CACHE_FILE", str(cache_file))
        monkeypatch.setattr(geocode_helper, "GOOGLE_MAPS_API_KEY", "TEST_KEY")

        with patch.object(geocode_helper, "geocode_address", new_callable=AsyncMock) as mock_ga:
            mock_ga.return_value = (32.78, -96.80)
            self._run(geocode_helper.geocode_locations([_provider()]))

        saved = json.loads(cache_file.read_text())
        key = geocode_helper.address_key("123 Main St", "Dallas", "TX", "75201")
        assert key in saved
        assert saved[key]["latitude"] == pytest.approx(32.78)

    def test_multiple_locations_all_geocoded(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "geocode_cache.json"
        monkeypatch.setattr(geocode_helper, "CACHE_FILE", str(cache_file))
        monkeypatch.setattr(geocode_helper, "GOOGLE_MAPS_API_KEY", "TEST_KEY")

        locs = [
            _provider("1 Alpha St", "Austin", "TX", "78701"),
            _provider("2 Beta Ave", "Houston", "TX", "77001"),
            _provider("3 Gamma Blvd", "Chicago", "IL", "60601"),
        ]

        with patch.object(geocode_helper, "geocode_address", new_callable=AsyncMock) as mock_ga:
            mock_ga.side_effect = [
                (30.27, -97.74),
                (29.76, -95.37),
                (41.88, -87.63),
            ]
            result = self._run(geocode_helper.geocode_locations(locs))

        assert result[0]["latitude"] == pytest.approx(30.27)
        assert result[1]["latitude"] == pytest.approx(29.76)
        assert result[2]["latitude"] == pytest.approx(41.88)

    def test_none_result_on_failed_geocode_does_not_crash(self, tmp_path, monkeypatch):
        cache_file = tmp_path / "geocode_cache.json"
        monkeypatch.setattr(geocode_helper, "CACHE_FILE", str(cache_file))
        monkeypatch.setattr(geocode_helper, "GOOGLE_MAPS_API_KEY", "TEST_KEY")

        locs = [_provider("Nonsense Rd 9999", "Nowhere", "ZZ", "00000")]

        with patch.object(geocode_helper, "geocode_address", new_callable=AsyncMock) as mock_ga:
            mock_ga.return_value = (None, None)
            result = self._run(geocode_helper.geocode_locations(locs))

        assert result[0]["latitude"] is None
        assert result[0]["longitude"] is None

    def test_mixed_cached_and_uncached(self, tmp_path, monkeypatch):
        key = geocode_helper.address_key("1 Alpha St", "Austin", "TX", "78701")
        cache = {key: {"latitude": 30.27, "longitude": -97.74}}
        cache_file = tmp_path / "geocode_cache.json"
        cache_file.write_text(json.dumps(cache))

        monkeypatch.setattr(geocode_helper, "CACHE_FILE", str(cache_file))
        monkeypatch.setattr(geocode_helper, "GOOGLE_MAPS_API_KEY", "TEST_KEY")

        locs = [
            _provider("1 Alpha St", "Austin", "TX", "78701"),   # cached
            _provider("2 Beta Ave", "Houston", "TX", "77001"),  # uncached
        ]

        with patch.object(geocode_helper, "geocode_address", new_callable=AsyncMock) as mock_ga:
            mock_ga.return_value = (29.76, -95.37)
            result = self._run(geocode_helper.geocode_locations(locs))
            assert mock_ga.call_count == 1  # only the uncached one

        assert result[0]["latitude"] == pytest.approx(30.27)   # from cache
        assert result[1]["latitude"] == pytest.approx(29.76)   # freshly geocoded
