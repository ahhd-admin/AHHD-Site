import { Search, MapPin, Map, List, Crosshair } from 'lucide-react';
import { useState, useEffect, useRef } from 'react';
import { APIProvider, useMapsLibrary } from '@vis.gl/react-google-maps';
import MapSearch from './MapSearch';
import ProviderCard from './ProviderCard';
import { supabase } from '../lib/supabase';
import type { LocationWithDetails } from '../types/database';
import { calculateDistance } from '../lib/geoUtils';
import { ALL_SERVICES } from '../lib/serviceCategories';
import { resolveStateCode } from '../lib/usStates';

const GOOGLE_MAPS_API_KEY = import.meta.env.VITE_GOOGLE_MAPS_API_KEY || '';

// A country-level search ("USA", "United States") should show nationwide
// results, not be treated as a literal city/state/zip substring to match
// against -- no real city, state abbreviation, or zip contains "usa".
const BROAD_LOCATION_TERMS = new Set(['usa', 'us', 'u.s.', 'u.s.a.', 'united states', 'united states of america', 'america']);

// Cap on rows fetched per search -- high enough to cover the full current
// dataset (~7,200 MVP-scope locations) so "no filter" genuinely means "show
// everything," not an arbitrary small slice of it.
const RESULTS_LIMIT = 8000;

interface SearchHeroProps {
  onSearch?: (location: string, serviceType?: string) => void;
}

function SearchHeroContent({ onSearch }: SearchHeroProps) {
  const [location, setLocation] = useState('');
  // Starts with nothing checked -- picking at least one Type of Care is a
  // deliberate part of the search now (see loadLocations: zero services
  // selected shows nothing rather than silently searching every service,
  // which also avoids surfacing older, out-of-scope data in the same
  // table that happens to share the same publish flags).
  const [selectedServices, setSelectedServices] = useState<string[]>([]);
  const [viewMode, setViewMode] = useState<'map' | 'list'>('map');
  const [locations, setLocations] = useState<LocationWithDetails[]>([]);
  const [loading, setLoading] = useState(false);
  const [distanceRadius, setDistanceRadius] = useState<number>(999999);
  // True once Search/Enter/a suggestion/"My Location" has actually been
  // used -- typing alone (or toggling Type of Care) doesn't set this, so
  // the map stays empty and the search bar stays centered until there's a
  // deliberate submit, instead of populating pins on every keystroke.
  const [hasSubmittedSearch, setHasSubmittedSearch] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const placesLibrary = useMapsLibrary('places');
  const [userCoords, setUserCoords] = useState<{ lat: number; lng: number } | null>(null);
  // The real geographic extent of the current search result (a whole
  // state, a city, a zip) -- MapSearch fits the map to this when present,
  // rather than a fixed close-in zoom, so a state search shows the whole
  // state instead of zooming in on one point in it.
  const [searchBounds, setSearchBounds] = useState<{ south: number; west: number; north: number; east: number } | null>(null);
  const [gettingLocation, setGettingLocation] = useState(false);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [suggestions, setSuggestions] = useState<google.maps.places.AutocompletePrediction[]>([]);
  const autocompleteService = useRef<google.maps.places.AutocompleteService | null>(null);
  const suggestionsRef = useRef<HTMLDivElement>(null);

  // Once a search has actually been submitted, refining Type of Care still
  // auto-refreshes live (debounced) without needing to hit Search again --
  // but nothing fetches before that first submit, and retyping the
  // location text doesn't refetch either (location changes only take
  // effect on the next explicit submit, which is also what re-geocodes for
  // the map's zoom/center). hasSubmittedSearch is deliberately read but not
  // listed as a dependency: handleSearch/handleSelectSuggestion/
  // handleGeolocation already fire the *first* fetch directly, so this
  // effect only needs to react to selectedServices changing thereafter --
  // listing hasSubmittedSearch too would double-fetch the moment a search
  // is first submitted.
  useEffect(() => {
    if (!hasSubmittedSearch) return;
    const timer = setTimeout(() => {
      loadLocations(location, selectedServices);
    }, 400);
    return () => clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedServices]);

  useEffect(() => {
    if (placesLibrary) {
      autocompleteService.current = new placesLibrary.AutocompleteService();
    }
  }, [placesLibrary]);

  useEffect(() => {
    const fetchSuggestions = async () => {
      if (!autocompleteService.current || !location.trim()) {
        setSuggestions([]);
        return;
      }

      try {
        const request = {
          input: location,
          componentRestrictions: { country: 'us' }
        };

        autocompleteService.current.getPlacePredictions(request, (predictions, status) => {
          if (status === google.maps.places.PlacesServiceStatus.OK && predictions) {
            setSuggestions(predictions.slice(0, 5));
          } else {
            setSuggestions([]);
          }
        });
      } catch (error) {
        console.error('Error fetching suggestions:', error);
      }
    };

    const timer = setTimeout(fetchSuggestions, 300);
    return () => clearTimeout(timer);
  }, [location]);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        suggestionsRef.current &&
        !suggestionsRef.current.contains(event.target as Node) &&
        !inputRef.current?.contains(event.target as Node)
      ) {
        setShowSuggestions(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const loadLocations = async (searchText: string, services: string[]) => {
    if (services.length === 0) {
      // No care type checked -- show nothing rather than dropping the
      // service filter entirely, which would silently surface older,
      // out-of-scope data that happens to share the same publish flags.
      setLocations([]);
      return;
    }

    setLoading(true);
    try {
      let query = supabase
        .from('locations')
        .select(`
          *,
          organization:organizations(organization_id,organization_name,website_url),
          service_types:location_service_types(
            service_type:service_types(service_type_id,service_type_name,service_type_slug)
          ),
          accreditation_records(accreditation_id,accrediting_body,accreditation_status,is_current_record)
        `)
        .eq('listing_status', 'published')
        .eq('accepts_public_display', true);

      const trimmed = searchText.trim();
      if (trimmed && !BROAD_LOCATION_TERMS.has(trimmed.toLowerCase())) {
        // Google Places descriptions (and plenty of manually-typed searches)
        // are comma-separated -- "Anchorage, AK, USA", "Alaska, USA",
        // "99501, USA", "123 Main St, Anchorage, AK 99501, USA". Splitting
        // on commas and matching each part against city/state/zip (instead
        // of the whole string as one substring) means any of those forms
        // works, not just a bare city name. A trailing country part is
        // dropped so it can't suppress otherwise-valid matches.
        const parts = trimmed
          .split(',')
          .map((p) => p.trim())
          .filter((p) => p && !BROAD_LOCATION_TERMS.has(p.toLowerCase()));

        const orParts: string[] = [];
        for (const rawPart of parts.length > 0 ? parts : [trimmed]) {
          const part = rawPart.replace(/[%]/g, '');
          if (!part) continue;
          orParts.push(`city.ilike.%${part}%`);
          orParts.push(`postal_code.ilike.%${part}%`);

          // The `state` column only ever stores 2-letter codes ("AK"), so a
          // typed full name ("Alaska") has to be translated first or it can
          // never match. resolveStateCode also accepts an already-valid
          // code, so this is safe to run unconditionally.
          const stateCode = resolveStateCode(part);
          orParts.push(`state.ilike.%${stateCode ?? part}%`);
        }

        if (orParts.length > 0) {
          query = query.or(orParts.join(','));
        }
      }

      if (services.length > 0) {
        // Filtering through a nested embed requires an inner join on both
        // hops, referenced by table name (not the select alias) in the
        // filter path -- verified directly against the live schema.
        query = query
          .select(`
            *,
            organization:organizations(organization_id,organization_name,website_url),
            service_types:location_service_types!inner(
              service_type:service_types!inner(service_type_id,service_type_name,service_type_slug)
            ),
            accreditation_records(accreditation_id,accrediting_body,accreditation_status,is_current_record)
          `)
          .in('location_service_types.service_types.service_type_slug', services);
      }

      const { data, error } = await query.limit(RESULTS_LIMIT);

      if (error) throw error;

      const mapped = data?.map((loc: any) => ({
        ...loc,
        service_types: loc.service_types?.map((st: any) => st.service_type).filter(Boolean) || []
      })) || [];

      setLocations(mapped);
    } catch (error) {
      console.error('Error loading locations:', error);
    } finally {
      setLoading(false);
    }
  };

  const toggleService = (serviceSlug: string) => {
    setSelectedServices((prev) =>
      prev.includes(serviceSlug)
        ? prev.filter((s) => s !== serviceSlug)
        : [...prev, serviceSlug]
    );
  };

  const allServiceSlugs = Object.keys(ALL_SERVICES);
  const allServicesSelected = allServiceSlugs.every((s) => selectedServices.includes(s));

  const toggleAllServices = () => {
    setSelectedServices(allServicesSelected ? [] : allServiceSlugs);
  };

  // Shared between the centered hero card (compact) and the post-search
  // filters row (full-size) so Type of Care can be picked before the very
  // first search, not just to refine an already-populated map.
  const renderCareTypeCheckboxes = (compact: boolean) => (
    <div
      className={`flex flex-wrap items-center gap-x-4 gap-y-2 border-2 border-neutral-300 rounded-xl bg-white ${
        compact ? 'px-3 py-1.5 min-h-[40px]' : 'px-3 py-2 min-h-[44px]'
      }`}
    >
      <label className="flex items-center gap-1.5 cursor-pointer">
        <input
          type="checkbox"
          checked={allServicesSelected}
          onChange={toggleAllServices}
          className="w-4 h-4 rounded border-neutral-300 text-primary-500 focus:ring-2 focus:ring-primary-200"
        />
        <span className={`font-semibold text-neutral-900 ${compact ? 'text-xs' : 'text-sm'}`}>All Care</span>
      </label>
      <span className="hidden sm:block w-px h-5 bg-neutral-300" aria-hidden="true" />
      {allServiceSlugs.map((slug) => (
        <label key={slug} className="flex items-center gap-1.5 cursor-pointer">
          <input
            type="checkbox"
            checked={selectedServices.includes(slug)}
            onChange={() => toggleService(slug)}
            className="w-4 h-4 rounded border-neutral-300 text-primary-500 focus:ring-2 focus:ring-primary-200"
          />
          <span className={`text-neutral-700 ${compact ? 'text-xs' : 'text-sm'}`}>{ALL_SERVICES[slug]}</span>
        </label>
      ))}
    </div>
  );

  const handleSelectSuggestion = async (placeId: string, description: string) => {
    // Fills the field and quietly geocodes/caches coordinates for later --
    // does NOT submit the search. Picking a suggestion used to jump
    // straight to results, which meant there was no chance to pick Type of
    // Care first; this makes it behave exactly like typing does, so there
    // is one consistent rule regardless of how the location got there: an
    // explicit Search click is what commits it. handleSearch already skips
    // re-geocoding when userCoords is set, so this isn't a wasted call --
    // it's just done ahead of time instead of at submit.
    setLocation(description);
    setShowSuggestions(false);

    if (placesLibrary) {
      const geocoder = new google.maps.Geocoder();
      geocoder.geocode({ placeId }, (results, status) => {
        if (status === 'OK' && results && results[0]) {
          const coords = {
            lat: results[0].geometry.location.lat(),
            lng: results[0].geometry.location.lng()
          };
          setUserCoords(coords);

          // bounds is the real extent for a region result (state, city,
          // zip); a street address only gets viewport, a smaller box
          // around the point -- either way this is a LatLngBounds
          // instance from the JS SDK, so its corners come from getters,
          // not raw JSON properties like the REST-based geocode below.
          const box = results[0].geometry.bounds || results[0].geometry.viewport;
          setSearchBounds(
            box
              ? {
                  south: box.getSouthWest().lat(),
                  west: box.getSouthWest().lng(),
                  north: box.getNorthEast().lat(),
                  east: box.getNorthEast().lng(),
                }
              : null
          );
        }
      });
    }
  };

  const handleSearch = async () => {
    setShowSuggestions(false);
    if (!location.trim()) return;
    setHasSubmittedSearch(true);

    if (!userCoords) {
      try {
        const response = await fetch(
          `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(location)}&key=${GOOGLE_MAPS_API_KEY}`
        );
        const data = await response.json();

        if (data.status === 'OK' && data.results && data.results.length > 0) {
          const result = data.results[0];
          const coords = {
            lat: result.geometry.location.lat,
            lng: result.geometry.location.lng
          };
          setUserCoords(coords);

          const box = result.geometry.bounds || result.geometry.viewport;
          setSearchBounds(
            box
              ? {
                  south: box.southwest.lat,
                  west: box.southwest.lng,
                  north: box.northeast.lat,
                  east: box.northeast.lng,
                }
              : null
          );
        }
      } catch (error) {
        console.error('Geocoding error:', error);
      }
    }
    loadLocations(location, selectedServices);
  };

  const handleGeolocation = () => {
    if (!navigator.geolocation) {
      alert('Geolocation is not supported by your browser');
      return;
    }

    setGettingLocation(true);
    navigator.geolocation.getCurrentPosition(
      async (position) => {
        const coords = {
          lat: position.coords.latitude,
          lng: position.coords.longitude
        };
        setUserCoords(coords);
        // GPS position is a real point, not a region -- clear any bounds
        // left over from a previous state/city search so the map zooms in
        // close on "near me" instead of re-fitting a stale region.
        setSearchBounds(null);
        setHasSubmittedSearch(true);

        try {
          const response = await fetch(
            `https://maps.googleapis.com/maps/api/geocode/json?latlng=${coords.lat},${coords.lng}&key=${import.meta.env.VITE_GOOGLE_MAPS_API_KEY}`
          );
          const data = await response.json();

          if (data.status === 'OK' && data.results && data.results.length > 0) {
            const cityResult = data.results.find((r: any) =>
              r.types.includes('locality') || r.types.includes('postal_code')
            ) || data.results[0];

            setLocation(cityResult.formatted_address);
            // Not delegating to handleSearch() here -- setLocation() above
            // hasn't committed yet by the time a synchronous call would
            // read `location` from this closure, so it would search with
            // the previous (likely empty) text. Using the just-resolved
            // address directly instead.
            loadLocations(cityResult.formatted_address, selectedServices);
          }
        } catch (error) {
          console.error('Reverse geocoding error:', error);
        } finally {
          setGettingLocation(false);
        }
      },
      (error) => {
        console.error('Geolocation error:', error);
        alert('Unable to get your location. Please enter it manually.');
        setGettingLocation(false);
      }
    );
  };

  const locationsWithDistance = locations.map((loc) => {
    let distance: number | null = null;
    if (userCoords && loc.latitude && loc.longitude) {
      distance = calculateDistance(userCoords.lat, userCoords.lng, loc.latitude, loc.longitude);
    }
    return { ...loc, distance };
  });

  // Location text and service-type filtering already happened server-side
  // in loadLocations -- `locations` only ever contains matching rows, so
  // this just applies the distance radius (which needs the client-computed
  // distance above) and sorts.
  const filteredLocations = locationsWithDistance.filter((loc) => {
    if (distanceRadius < 999999 && loc.distance !== null) {
      if (loc.distance > distanceRadius) {
        return false;
      }
    }

    return true;
  }).sort((a, b) => {
    // display_order was never a real column on locations (it belongs to
    // service_types) -- this always fell through to distance sorting in
    // practice. If per-location manual ordering is wanted later, it needs
    // a real column on locations or location_listing_settings first.
    if (a.distance === null && b.distance === null) return 0;
    if (a.distance === null) return 1;
    if (b.distance === null) return -1;
    return a.distance - b.distance;
  });

  return (
    <section className="bg-gradient-to-br from-primary-50 via-white to-navy-50 py-6 md:py-10">
      <div className="max-w-7xl mx-auto px-3 sm:px-6 lg:px-8">
        <div className="text-center mb-4 md:mb-6">
          <h1 className="text-3xl md:text-4xl font-heading font-bold text-navy-800 mb-2">
            Find Quality Care Providers
          </h1>

          <p className="text-base text-neutral-700 max-w-3xl mx-auto leading-relaxed">
            Search accredited home care, hospice, and healthcare services in your area
          </p>
        </div>

        {/* The map is the hero -- search lives as a single floating bar on
            top of it instead of a separate form stacked above, so there's
            one place to type instead of two. Before a search it's centered
            with a prompt (nothing to show yet); once there's a location,
            it collapses to a slim bar pinned at the top so re-searching
            stays one click away without taking up permanent space. */}
        <div className="relative rounded-2xl overflow-hidden shadow-xl border border-neutral-200 bg-white">
          <div className={!hasSubmittedSearch ? 'pointer-events-none' : ''}>
            {loading ? (
              <div className="h-[500px] md:h-[600px] flex items-center justify-center bg-neutral-50">
                <div className="inline-block animate-spin rounded-full h-12 w-12 border-4 border-primary-200 border-t-primary-600"></div>
              </div>
            ) : viewMode === 'list' && hasSubmittedSearch ? (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 p-4 min-h-[500px] md:min-h-[600px] bg-neutral-50">
                {filteredLocations.map((loc) => (
                  <ProviderCard key={loc.location_id} location={loc} />
                ))}
              </div>
            ) : (
              <MapSearch
                locations={filteredLocations}
                userCoords={userCoords}
                searchBounds={searchBounds}
                radiusMiles={distanceRadius}
              />
            )}
          </div>

          {!hasSubmittedSearch && !loading && (
            <div className="absolute inset-0 bg-white/60" aria-hidden="true" />
          )}

          {/* A fixed-size floating card in both states, but not the SAME
              size -- max-w-2xl reads right anchored in the corner
              (surrounded by map/pin context), but the same width felt
              oversized as a big empty box centered on a dimmed map, so the
              centered pre-search state stays narrower (max-w-md). Was
              previously a full-width bar across the top after search,
              which covered the map's native fullscreen control (Google
              places it top-right by default); staying a corner card
              leaves that clear. */}
          <div
            className={`absolute z-20 transition-all duration-300 ${
              hasSubmittedSearch
                ? 'top-3 left-3 w-[calc(100%-1.5rem)] max-w-2xl'
                : 'top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[calc(100%-2rem)] max-w-md'
            }`}
          >
            <div className="bg-white rounded-xl shadow-2xl border border-neutral-200 p-3 sm:p-4">
              {!hasSubmittedSearch && (
                <div className="text-center mb-3">
                  <div className="inline-flex items-center justify-center w-12 h-12 bg-primary-100 rounded-2xl mb-2">
                    <MapPin className="w-6 h-6 text-navy-800" />
                  </div>
                  <p className="font-semibold text-navy-800">Where are you looking for care?</p>
                  <p className="text-sm text-neutral-600 mt-0.5">Enter a state, address, or ZIP code</p>
                </div>
              )}

              {/* Input gets its own full-width row, buttons wrap to a row
                  below on narrow screens -- previously the input shared a
                  row with two buttons even on small phones, squeezing it
                  down far enough that the placeholder text got clipped. */}
              <div className="flex flex-col sm:flex-row gap-2">
                <div className="flex-1 relative">
                  <MapPin className="absolute left-3 top-1/2 -translate-y-1/2 text-neutral-400 w-4 h-4 z-10 pointer-events-none" />
                  <input
                    ref={inputRef}
                    id="location-search"
                    type="text"
                    value={location}
                    onChange={(e) => {
                      const value = e.target.value;
                      setLocation(value);
                      setUserCoords(null);
                      setSearchBounds(null);
                      setShowSuggestions(true);
                      if (!value.trim()) {
                        // Cleared back to empty -- fully reset to the idle
                        // prompt state rather than leaving stale results
                        // showing behind a bar that no longer reflects it.
                        setHasSubmittedSearch(false);
                      }
                    }}
                    onFocus={() => setShowSuggestions(true)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        handleSearch();
                      } else if (e.key === 'Escape') {
                        setShowSuggestions(false);
                      }
                    }}
                    placeholder="City, state, or ZIP code"
                    aria-label="Search location"
                    className="w-full pl-9 pr-3 h-[48px] text-base border-2 border-neutral-300 rounded-lg focus:outline-none focus:ring-4 focus:ring-primary-200 focus:border-primary-500 transition-all"
                    autoComplete="off"
                  />
                  {showSuggestions && suggestions.length > 0 && (
                    <div
                      ref={suggestionsRef}
                      className="absolute z-[10000] mt-1 w-full bg-white border-2 border-neutral-300 rounded-xl shadow-2xl overflow-hidden"
                    >
                      {suggestions.map((suggestion) => (
                        <button
                          key={suggestion.place_id}
                          onClick={() => handleSelectSuggestion(suggestion.place_id, suggestion.description)}
                          className="w-full px-4 py-3 text-left hover:bg-primary-50 transition-colors border-b border-neutral-100 last:border-b-0 flex items-center gap-3"
                        >
                          <MapPin className="w-4 h-4 text-neutral-400 flex-shrink-0" />
                          <span className="text-neutral-800">{suggestion.description}</span>
                        </button>
                      ))}
                    </div>
                  )}
                </div>

                <div className="flex gap-2">
                  <button
                    onClick={handleGeolocation}
                    disabled={gettingLocation}
                    className="btn-outline flex-1 sm:flex-none px-3 h-[48px] flex items-center justify-center gap-1.5 whitespace-nowrap disabled:opacity-50 disabled:cursor-not-allowed"
                    aria-label="Use my location"
                    title="Use my location"
                  >
                    <Crosshair className={`w-4 h-4 ${gettingLocation ? 'animate-pulse' : ''}`} />
                    <span className="text-sm">My Location</span>
                  </button>

                  <button
                    onClick={handleSearch}
                    className="btn-primary flex-1 sm:flex-none px-3 sm:px-4 h-[48px] flex items-center justify-center gap-1.5 whitespace-nowrap"
                  >
                    <Search className="w-4 h-4" />
                    <span className="text-sm">Search</span>
                  </button>
                </div>
              </div>

              {/* Type of Care is pickable before the first search too, not
                  just to refine an already-populated map -- it's part of
                  what's being searched for, not an afterthought. */}
              {!hasSubmittedSearch && (
                <div className="mt-2">
                  <p className="text-xs font-semibold text-navy-800 mb-1">Type of Care</p>
                  {renderCareTypeCheckboxes(true)}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Radius and the map/list toggle only show up once there's
            something to refine -- but Type of Care lives in the hero card
            above before that point, since it's part of the search itself. */}
        {hasSubmittedSearch && (
          <div className="mt-3 flex flex-col sm:flex-row gap-3 items-start">
            <fieldset className="flex-1 w-full sm:w-auto border-0 p-0 m-0 min-w-0">
              <legend className="block text-sm font-semibold text-navy-800 mb-1.5">
                Type of Care
              </legend>
              {renderCareTypeCheckboxes(false)}
            </fieldset>

            <div className="flex gap-2 items-end w-full sm:w-auto">
              <div className="flex-1 sm:w-[130px]">
                <label htmlFor="radius-select" className="block text-sm font-semibold text-navy-800 mb-1.5">
                  Radius
                </label>
                <select
                  id="radius-select"
                  value={distanceRadius}
                  onChange={(e) => setDistanceRadius(Number(e.target.value))}
                  className="w-full px-3 h-[44px] text-sm border-2 border-neutral-300 rounded-xl focus:outline-none focus:ring-4 focus:ring-primary-200 focus:border-primary-500 transition-all bg-white text-neutral-800 font-medium"
                >
                  <option value={999999}>Any distance</option>
                  <option value={5}>Within 5 mi</option>
                  <option value={10}>Within 10 mi</option>
                  <option value={25}>Within 25 mi</option>
                  <option value={50}>Within 50 mi</option>
                  <option value={100}>Within 100 mi</option>
                </select>
              </div>

              <div className="flex items-center gap-1 bg-neutral-100 rounded-lg p-1 h-[44px]">
                <button
                  onClick={() => setViewMode('map')}
                  className={`flex items-center justify-center w-9 sm:w-auto sm:gap-1.5 sm:px-3 h-full rounded-md text-sm font-medium transition-all ${
                    viewMode === 'map'
                      ? 'bg-white text-navy-800 shadow-sm'
                      : 'text-neutral-600 hover:text-navy-800'
                  }`}
                  aria-label="Map view"
                  aria-pressed={viewMode === 'map'}
                >
                  <Map className="w-4 h-4" />
                  <span className="hidden sm:inline">Map</span>
                </button>
                <button
                  onClick={() => setViewMode('list')}
                  className={`flex items-center justify-center w-9 sm:w-auto sm:gap-1.5 sm:px-3 h-full rounded-md text-sm font-medium transition-all ${
                    viewMode === 'list'
                      ? 'bg-white text-navy-800 shadow-sm'
                      : 'text-neutral-600 hover:text-navy-800'
                  }`}
                  aria-label="List view"
                  aria-pressed={viewMode === 'list'}
                >
                  <List className="w-4 h-4" />
                  <span className="hidden sm:inline">List</span>
                </button>
              </div>
            </div>
          </div>
        )}

        {hasSubmittedSearch && filteredLocations.length === 0 && !loading && (
          <div className="mt-6 text-center">
            <p className="text-neutral-600">
              No providers found matching your criteria. Try adjusting your filters or search location.
            </p>
          </div>
        )}

        {filteredLocations.length > 0 && (
          <div className="mt-6 text-center">
            <p className="text-sm text-neutral-500">
              Showing {filteredLocations.length} accredited {filteredLocations.length === 1 ? 'provider' : 'providers'}
              {location && ` near ${location}`}
            </p>
          </div>
        )}
      </div>
    </section>
  );
}

export default function SearchHero({ onSearch }: SearchHeroProps) {
  return (
    <APIProvider apiKey={GOOGLE_MAPS_API_KEY} libraries={['places']}>
      <SearchHeroContent onSearch={onSearch} />
    </APIProvider>
  );
}
