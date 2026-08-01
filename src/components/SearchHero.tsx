import { Search, MapPin, Map, List, ChevronDown, Filter, X, Crosshair } from 'lucide-react';
import { useState, useEffect, useRef } from 'react';
import { APIProvider, useMapsLibrary } from '@vis.gl/react-google-maps';
import MapSearch from './MapSearch';
import ProviderCard from './ProviderCard';
import { supabase } from '../lib/supabase';
import type { LocationWithDetails } from '../types/database';
import { calculateDistance } from '../lib/geoUtils';
import { SERVICE_CATEGORIES, ALL_SERVICES } from '../lib/serviceCategories';
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
  // Defaults to every MVP-scope service rather than none, so the initial
  // view (and any search with no service checkboxes touched) is naturally
  // scoped to real, current data -- there's also older, unrelated data in
  // the same table (different service categories, months out of date) that
  // happens to share the same published/publicly-displayable flags; an
  // empty filter would show that too.
  const [selectedServices, setSelectedServices] = useState<string[]>(Object.keys(ALL_SERVICES));
  const [showFilterMenu, setShowFilterMenu] = useState(false);
  const filterMenuRef = useRef<HTMLDivElement>(null);
  const [viewMode, setViewMode] = useState<'map' | 'list'>('map');
  const [locations, setLocations] = useState<LocationWithDetails[]>([]);
  const [loading, setLoading] = useState(false);
  const [hasSearched, setHasSearched] = useState(false);
  const [distanceRadius, setDistanceRadius] = useState<number>(999999);
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

  // Auto-search as you type/select filters, debounced -- no need to hit
  // Search first. handleSearch (button/Enter) still runs on top of this for
  // geocoding-based map centering and "near me" distance sorting.
  useEffect(() => {
    const timer = setTimeout(() => {
      loadLocations(location, selectedServices);
    }, 400);
    return () => clearTimeout(timer);
  }, [location, selectedServices]);

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
        filterMenuRef.current &&
        !filterMenuRef.current.contains(event.target as Node)
      ) {
        setShowFilterMenu(false);
      }
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

  const toggleCategory = (categoryKey: string) => {
    const category = SERVICE_CATEGORIES[categoryKey as keyof typeof SERVICE_CATEGORIES];
    const allSelected = category.services.every(s => selectedServices.includes(s));

    if (allSelected) {
      setSelectedServices(prev => prev.filter(s => !category.services.includes(s)));
    } else {
      setSelectedServices(prev => {
        const filtered = prev.filter(s => !category.services.includes(s));
        return [...filtered, ...category.services];
      });
    }
  };

  const removeFilter = (serviceSlug: string) => {
    setSelectedServices(prev => prev.filter(s => s !== serviceSlug));
  };

  const clearAllFilters = () => {
    setSelectedServices([]);
  };

  const handleSelectSuggestion = async (placeId: string, description: string) => {
    setLocation(description);
    setShowSuggestions(false);
    setHasSearched(true);

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

          loadLocations(description, selectedServices);
        }
      });
    }
  };

  const handleSearch = async () => {
    setShowSuggestions(false);
    setHasSearched(true);

    if (location.trim() && !userCoords) {
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
    setHasSearched(true);
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
            handleSearch();
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
    <section className="bg-gradient-to-br from-primary-50 via-white to-navy-50 py-8 md:py-16">
      <div className="max-w-7xl mx-auto px-3 sm:px-6 lg:px-8">
        <div className="text-center mb-8">
          <h1 className="text-4xl md:text-5xl font-heading font-bold text-navy-800 mb-4">
            Find Quality Care Providers
          </h1>

          <p className="text-lg text-neutral-700 max-w-3xl mx-auto leading-relaxed">
            Search accredited home care, hospice, and healthcare services in your area
          </p>
        </div>

        <div className="bg-white rounded-2xl shadow-xl border border-neutral-200 overflow-hidden">
          <div className="p-4 md:p-6 border-b border-neutral-200">
            <div className="mb-4">
              <label htmlFor="location-search" className="block text-sm font-semibold text-navy-800 mb-2">
                Location
              </label>
              <div className="flex gap-2">
                <div className="flex-1 relative">
                  <MapPin className="absolute left-4 top-1/2 -translate-y-1/2 text-neutral-400 w-5 h-5 z-10 pointer-events-none" />
                  <input
                    ref={inputRef}
                    id="location-search"
                    type="text"
                    value={location}
                    onChange={(e) => {
                      setLocation(e.target.value);
                      setUserCoords(null);
                      setSearchBounds(null);
                      setShowSuggestions(true);
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
                    placeholder="Enter city, address, or ZIP code"
                    className="w-full pl-12 pr-24 md:pr-4 h-[50px] text-base border-2 border-neutral-300 rounded-xl focus:outline-none focus:ring-4 focus:ring-primary-200 focus:border-primary-500 transition-all"
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
                  <button
                    onClick={handleGeolocation}
                    disabled={gettingLocation}
                    className="md:hidden absolute right-11 top-1/2 -translate-y-1/2 p-2 text-neutral-500 hover:text-primary-600 disabled:opacity-50 transition-colors z-20"
                    aria-label="Use my location"
                    title="Use my location"
                  >
                    <Crosshair className={`w-5 h-5 ${gettingLocation ? 'animate-pulse' : ''}`} />
                  </button>
                  <button
                    onClick={handleSearch}
                    className="md:hidden absolute right-2 top-1/2 -translate-y-1/2 p-2 text-primary-600 hover:text-primary-700 transition-colors z-20"
                    aria-label="Search"
                  >
                    <Search className="w-5 h-5" />
                  </button>
                </div>

                <button
                  onClick={handleGeolocation}
                  disabled={gettingLocation}
                  className="hidden md:flex btn-outline px-4 h-[50px] items-center gap-2 whitespace-nowrap disabled:opacity-50 disabled:cursor-not-allowed"
                  title="Use my location"
                >
                  <Crosshair className={`w-4 h-4 ${gettingLocation ? 'animate-pulse' : ''}`} />
                  <span className="hidden lg:inline">My Location</span>
                </button>

                <button
                  onClick={handleSearch}
                  className="hidden md:flex btn-primary px-6 h-[50px] items-center gap-2 whitespace-nowrap"
                >
                  <Search className="w-4 h-4" />
                  Search
                </button>

                <div className="flex items-center gap-1 bg-neutral-100 rounded-lg p-1 h-[50px]">
                  <button
                    onClick={() => setViewMode('map')}
                    className={`flex items-center justify-center w-10 md:w-auto md:gap-1.5 md:px-3 h-full rounded-md text-sm font-medium transition-all ${
                      viewMode === 'map'
                        ? 'bg-white text-navy-800 shadow-sm'
                        : 'text-neutral-600 hover:text-navy-800'
                    }`}
                    aria-label="Map view"
                  >
                    <Map className="w-4 h-4" />
                    <span className="hidden md:inline">Map</span>
                  </button>
                  <button
                    onClick={() => setViewMode('list')}
                    className={`flex items-center justify-center w-10 md:w-auto md:gap-1.5 md:px-3 h-full rounded-md text-sm font-medium transition-all ${
                      viewMode === 'list'
                        ? 'bg-white text-navy-800 shadow-sm'
                        : 'text-neutral-600 hover:text-navy-800'
                    }`}
                    aria-label="List view"
                  >
                    <List className="w-4 h-4" />
                    <span className="hidden md:inline">List</span>
                  </button>
                </div>
              </div>
            </div>

            <div className="flex flex-col sm:flex-row gap-3 items-start">
              <div className="flex-1 w-full sm:w-auto" ref={filterMenuRef}>
                <label className="block text-sm font-semibold text-navy-800 mb-2">
                  Service Category
                </label>
                <button
                  type="button"
                  onClick={() => setShowFilterMenu(!showFilterMenu)}
                  className="w-full px-4 h-[50px] text-base border-2 border-neutral-300 rounded-xl focus:outline-none focus:ring-4 focus:ring-primary-200 focus:border-primary-500 transition-all bg-white flex items-center justify-between text-left"
                >
                  <div className="flex items-center gap-2">
                    <Filter className="w-4 h-4 text-neutral-500" />
                    <span className={selectedServices.length === 0 ? 'text-neutral-500' : 'text-neutral-800 font-medium'}>
                      {selectedServices.length === 0 ? 'Filter by service category' : `${selectedServices.length} selected`}
                    </span>
                  </div>
                  <ChevronDown className={`w-5 h-5 text-neutral-400 transition-transform ${showFilterMenu ? 'rotate-180' : ''}`} />
                </button>

                {showFilterMenu && (
                  <div className="absolute z-[10000] mt-2 bg-white border-2 border-neutral-300 rounded-xl shadow-2xl overflow-hidden w-[320px] max-h-[500px] overflow-y-auto">
                    {Object.entries(SERVICE_CATEGORIES).map(([categoryKey, category]) => {
                      const allSelected = category.services.every(s => selectedServices.includes(s));
                      const someSelected = category.services.some(s => selectedServices.includes(s));

                      return (
                        <div key={categoryKey} className="border-b border-neutral-200 last:border-b-0">
                          <label className="flex items-center px-4 py-3 hover:bg-primary-50 cursor-pointer bg-neutral-50">
                            <input
                              type="checkbox"
                              checked={allSelected}
                              onChange={() => toggleCategory(categoryKey)}
                              className="w-4 h-4 rounded border-neutral-300 text-primary-500 focus:ring-2 focus:ring-primary-200 mr-3 flex-shrink-0"
                              style={{ opacity: someSelected && !allSelected ? 0.5 : 1 }}
                            />
                            <span className="text-neutral-900 font-semibold leading-none">{category.label}</span>
                          </label>
                          <div className="pl-4">
                            {category.services.map((serviceSlug) => (
                              <label key={serviceSlug} className="flex items-center px-4 py-2.5 hover:bg-primary-50 cursor-pointer">
                                <input
                                  type="checkbox"
                                  checked={selectedServices.includes(serviceSlug)}
                                  onChange={() => toggleService(serviceSlug)}
                                  className="w-4 h-4 rounded border-neutral-300 text-primary-500 focus:ring-2 focus:ring-primary-200 mr-3 flex-shrink-0"
                                />
                                <span className="text-neutral-700 text-sm leading-none">{ALL_SERVICES[serviceSlug as keyof typeof ALL_SERVICES]}</span>
                              </label>
                            ))}
                          </div>
                        </div>
                      );
                    })}
                    {selectedServices.length > 0 && (
                      <div className="p-3 bg-neutral-50 border-t border-neutral-200">
                        <button
                          onClick={clearAllFilters}
                          className="w-full py-2 px-3 text-sm font-medium text-neutral-700 hover:text-navy-800 transition-colors"
                        >
                          Clear all filters
                        </button>
                      </div>
                    )}
                  </div>
                )}
              </div>

              <div className="w-full sm:w-[140px]">
                <label className="block text-sm font-semibold text-navy-800 mb-2">
                  Radius
                </label>
                <select
                  value={distanceRadius}
                  onChange={(e) => setDistanceRadius(Number(e.target.value))}
                  className="w-full px-3 h-[50px] text-sm border-2 border-neutral-300 rounded-xl focus:outline-none focus:ring-4 focus:ring-primary-200 focus:border-primary-500 transition-all bg-white text-neutral-800 font-medium"
                >
                  <option value={999999}>Any distance</option>
                  <option value={5}>Within 5 mi</option>
                  <option value={10}>Within 10 mi</option>
                  <option value={25}>Within 25 mi</option>
                  <option value={50}>Within 50 mi</option>
                  <option value={100}>Within 100 mi</option>
                </select>
              </div>
            </div>

            {selectedServices.length > 0 && (
              <div className="mt-4 pt-4 border-t border-neutral-200">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-semibold text-navy-800">Active Filters</span>
                  <button
                    onClick={clearAllFilters}
                    className="text-xs text-neutral-600 hover:text-navy-800 font-medium transition-colors"
                  >
                    Clear all
                  </button>
                </div>
                <div className="flex flex-wrap gap-2">
                  {selectedServices.map((serviceSlug) => (
                    <span
                      key={serviceSlug}
                      className="inline-flex items-center gap-1.5 px-3 py-1.5 bg-primary-100 text-primary-700 rounded-lg text-sm font-medium"
                    >
                      {ALL_SERVICES[serviceSlug as keyof typeof ALL_SERVICES]}
                      <button
                        onClick={() => removeFilter(serviceSlug)}
                        className="hover:bg-primary-200 rounded-full p-0.5 transition-colors"
                        aria-label={`Remove ${ALL_SERVICES[serviceSlug as keyof typeof ALL_SERVICES]} filter`}
                      >
                        <X className="w-3 h-3" />
                      </button>
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>

          <div className="bg-neutral-50 p-4">
            {loading ? (
              <div className="flex items-center justify-center py-20">
                <div className="inline-block animate-spin rounded-full h-12 w-12 border-4 border-primary-200 border-t-primary-600"></div>
              </div>
            ) : viewMode === 'map' ? (
              <MapSearch
                locations={filteredLocations}
                userCoords={userCoords}
                searchBounds={searchBounds}
                radiusMiles={distanceRadius}
              />
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {filteredLocations.map((location) => (
                  <ProviderCard key={location.location_id} location={location} />
                ))}
              </div>
            )}
          </div>
        </div>

        {filteredLocations.length === 0 && !loading && hasSearched && (
          <div className="mt-6 text-center">
            <p className="text-neutral-600">
              No providers found matching your criteria. Try adjusting your filters or search location.
            </p>
          </div>
        )}

        {filteredLocations.length === 0 && !loading && !hasSearched && (
          <div className="mt-6 text-center">
            <div className="inline-flex items-center justify-center w-16 h-16 bg-primary-100 rounded-2xl mb-4">
              <Search className="w-8 h-8 text-primary-600" />
            </div>
            <p className="text-lg font-semibold text-navy-800 mb-2">
              Start your search above
            </p>
            <p className="text-neutral-600 max-w-md mx-auto">
              Enter a city, address, or ZIP code to find accredited healthcare providers near you. You can also filter by service category and distance.
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
