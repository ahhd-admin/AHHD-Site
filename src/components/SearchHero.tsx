import { Search, MapPin, Map, LayoutGrid, Crosshair, HelpCircle } from 'lucide-react';
import { useState, useEffect, useRef } from 'react';
import { APIProvider, useMapsLibrary } from '@vis.gl/react-google-maps';
import MapSearch from './MapSearch';
import ProviderCard from './ProviderCard';
import TrustStats from './TrustStats';
import { supabase } from '../lib/supabase';
import type { LocationWithDetails } from '../types/database';
import { calculateDistance } from '../lib/geoUtils';
import { ALL_SERVICES } from '../lib/serviceCategories';
import { resolveStateCode, extractStateCode, stateNameFromCode, findUniqueStateMatch } from '../lib/usStates';
import { HOME_SCROLL_STORAGE_KEY } from '../lib/scrollRestoration';
import { saveSearchCache, loadSearchCache } from '../lib/searchResultsCache';
import { fetchSearchBoundary, fetchStateBoundary, computeBoundsFromGeoJSON, computeCenterFromBounds, type BoundaryGeoJSON } from '../lib/boundaryLookup';

const GOOGLE_MAPS_API_KEY = import.meta.env.VITE_GOOGLE_MAPS_API_KEY || '';

// A country-level search ("USA", "United States") should show nationwide
// results, not be treated as a literal city/state/zip substring to match
// against -- no real city, state abbreviation, or zip contains "usa".
const BROAD_LOCATION_TERMS = new Set(['usa', 'us', 'u.s.', 'u.s.a.', 'united states', 'united states of america', 'america']);

// Marks the synthetic "[State] (entire state)" autocomplete suggestion
// (see the suggestions-fetch effect and handleSelectSuggestion) as
// distinct from a real Google place_id, which is always a long opaque
// string starting with "ChIJ" or similar -- never this literal prefix.
const DIRECT_STATE_PLACE_ID_PREFIX = '__direct_state__';

// Cap on rows fetched per search -- high enough to cover the full current
// dataset (~7,200 MVP-scope locations) so "no filter" genuinely means "show
// everything," not an arbitrary small slice of it.
const RESULTS_LIMIT = 8000;

// Half-width (in degrees) of the geographic box fetched around a
// geocoded point, sized to how specific the search actually is -- a city
// search benefits from a wide "surrounding metro area" box (a city or zip
// search used to filter by literal text match on the city name, which
// meant "Chicago, IL" excluded every real suburb -- Evanston, Oak Park,
// Naperville -- that doesn't happen to contain "Chicago" in its name),
// but reusing that same wide box for a single ZIP code search meant
// searching one Chicago ZIP code still showed almost the entire metro
// area, spilling into Indiana/Michigan/Wisconsin -- a ZIP is a
// specific, few-square-mile area, not a whole city. 1 degree of latitude
// is a near-constant ~69 miles everywhere; longitude varies with
// latitude (shorter near the poles), but for the contiguous US this
// still lands close enough to use uniformly -- this is "show the right
// scale of area for what was searched," not a precision distance tool
// (the Radius selector, filtered client-side against the real calculated
// distance, is what gives an exact answer).
const CITY_RADIUS_DEGREES = 0.75; // ~50mi -- city/locality searches
const ZIP_RADIUS_DEGREES = 0.12; // ~8mi -- a ZIP code's own scale
const ADDRESS_RADIUS_DEGREES = 0.06; // ~4mi -- an exact address, tighter still
const MILES_PER_DEGREE = 69;

// Tried in order (each added on top of the original searched box, not
// stacked on the previous attempt) when a search comes back with zero
// results -- see loadLocations. Matches the Radius selector's own step
// options so "the search auto-widened to N mi" and "pick N mi yourself"
// land on the same values.
const RADIUS_EXPANSION_STEPS = [5, 10, 25, 50, 100];

// Expands a bounding box outward by a fixed mile margin on every side --
// used for a state search's Radius selector: "50 mi" should mean a
// buffer around the state's actual (irregular) shape, not a circle
// centered on its centroid. Centroid-distance is a poor proxy for a
// large or elongated state -- a location near Illinois's northern tip
// can be 200+ miles from the state's centroid even though it's well
// inside the state, so filtering that way would wrongly exclude
// genuinely-in-state results. This isn't a true polygon buffer (that
// would need to offset the state's actual irregular outline, not just
// its bounding rectangle), but expanding the rectangle is a reasonable
// approximation and the rectangle is already what's actually queried.
function expandBoundsByMiles(
  bounds: { south: number; west: number; north: number; east: number },
  miles: number
): { south: number; west: number; north: number; east: number } {
  if (miles >= 999999) return bounds; // "Any distance" -- the state's own extent, no added margin
  const degrees = miles / MILES_PER_DEGREE;
  return {
    south: bounds.south - degrees,
    north: bounds.north + degrees,
    west: bounds.west - degrees,
    east: bounds.east + degrees,
  };
}

// See src/lib/scrollRestoration.ts for the full explanation -- the
// browser's own scroll restoration fires before this page's async
// results fetch resolves, landing on the wrong position. Overriding it
// to 'manual' here so only the explicit restore effect below (which waits
// for results to actually render) ever moves the scroll position.
if (typeof window !== 'undefined' && 'scrollRestoration' in window.history) {
  window.history.scrollRestoration = 'manual';
}

interface SearchHeroProps {
  onSearch?: (location: string, serviceType?: string) => void;
}

// Site has no client-side router (Router.tsx drives entirely off
// window.location.pathname/pushState, and provider links are plain <a
// href> tags -- a real page load/reload, not an SPA route change), so a
// browser back navigation from a provider page fully remounts this
// component. Persisting search state in the URL's query string is what
// survives that: the query string is part of the URL itself, restored by
// the browser regardless of the reload. Read once per mount, not via
// lazy useState initializers, since it's a handful of cheap string reads.
const initialSearchParams = new URLSearchParams(window.location.search);
const initialLocationParam = initialSearchParams.get('location') || '';
const initialCareParam = initialSearchParams.get('care');
const initialRadiusParam = initialSearchParams.get('radius');

function SearchHeroContent({ onSearch }: SearchHeroProps) {
  const [location, setLocation] = useState(initialLocationParam);
  // Defaults to every service checked -- one fewer thing to set up before
  // searching. Nothing shows until a location is actually searched
  // regardless (hasSubmittedSearch below), so this doesn't skip that
  // gate; it just means the common case (search a location, see
  // everything) takes one less click. ensureServicesSelected() still
  // covers the edge case of someone manually unchecking every box and
  // then submitting.
  const [selectedServices, setSelectedServices] = useState<string[]>(
    // Validated against ALL_SERVICES, not passed straight through --
    // `care` comes from the URL query string, and the query below skips
    // the service-type filter entirely once at least one slug is
    // present. An unvalidated out-of-scope slug (e.g. a hand-typed or
    // bookmarked `?care=pharmacy`) would otherwise surface the
    // out-of-scope categories still sitting in Supabase from before the
    // MVP scope decision (see serviceCategories.ts) despite the
    // checkbox UI itself never offering them.
    initialCareParam
      ? initialCareParam.split(',').filter((slug) => slug in ALL_SERVICES)
      : Object.keys(ALL_SERVICES)
  );
  const [viewMode, setViewMode] = useState<'map' | 'list'>(
    initialSearchParams.get('view') === 'list' ? 'list' : 'map'
  );
  const [locations, setLocations] = useState<LocationWithDetails[]>([]);
  const [loading, setLoading] = useState(false);
  const [distanceRadius, setDistanceRadius] = useState<number>(
    initialRadiusParam ? Number(initialRadiusParam) : 999999
  );
  // Set when loadLocations's auto-expand-on-zero-results actually widened
  // the search (see RADIUS_EXPANSION_STEPS below) -- previously the Radius
  // dropdown just silently changed to a wider value with no explanation,
  // which read as the app ignoring what was actually searched rather than
  // honestly saying "nothing right here, so here's what's nearby instead."
  const [autoExpandNotice, setAutoExpandNotice] = useState<string | null>(null);
  // True once Search/Enter/a suggestion/"My Location" has actually been
  // used -- typing alone (or toggling Type of Care) doesn't set this, so
  // the map stays empty and the search bar stays centered until there's a
  // deliberate submit, instead of populating pins on every keystroke.
  // Starts true if a location came in from the URL (a restored search),
  // so the map/grid populates immediately instead of showing the empty
  // prompt for a location that's already known.
  const [hasSubmittedSearch, setHasSubmittedSearch] = useState(!!initialLocationParam);
  // Bumped only on a genuine new submit (Search/Enter/"My Location") --
  // NOT when Type of Care or other filters change the result set for the
  // same search. MapSearch uses this to know when it's allowed to move
  // the camera again vs. just update which pins are showing.
  const [searchGeneration, setSearchGeneration] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const placesLibrary = useMapsLibrary('places');
  const [userCoords, setUserCoords] = useState<{ lat: number; lng: number } | null>(null);
  // The real geographic extent of the current search result (a whole
  // state, a city, a zip) -- MapSearch fits the map to this when present,
  // rather than a fixed close-in zoom, so a state search shows the whole
  // state instead of zooming in on one point in it.
  const [searchBounds, setSearchBounds] = useState<{ south: number; west: number; north: number; east: number } | null>(null);
  // The real traced boundary shape for the current search (a city's
  // actual outline, a state's actual outline), when the boundary-lookup
  // Edge Function found one -- MapSearch draws this instead of the
  // rectangle/circle when present. Best-effort only: stays null (falls
  // back to the existing region indicator) for anything the lookup
  // doesn't cover -- most ZIP codes, or if the lookup fails for any
  // reason at all.
  const [boundaryPolygon, setBoundaryPolygon] = useState<BoundaryGeoJSON | null>(null);
  // True for the brief window a city/state search's real boundary trace is
  // in flight. MapSearch uses this to withhold the interim bounding-box
  // rectangle for that window instead of drawing it immediately and then
  // replacing it the moment the trace resolves -- previously read as a
  // large rectangle flashing over the map before the real outline (or,
  // often, nothing at all, since most traces resolve well under a second)
  // took its place. Never set for zip/address searches, which never
  // attempt a trace (see shouldTraceBoundary) and should keep showing
  // their rectangle immediately, same as before.
  const [boundaryLoading, setBoundaryLoading] = useState(false);
  // How specific the current search is -- drives whether the Radius
  // circle draws at all (only for zip/address; a wide circle over an
  // entire city/state just competes visually with its outline and adds
  // nothing a Radius selector user actually wants there), and whether
  // the Radius selector means "buffer around the whole region" (state)
  // vs. "distance from the searched point" (everything else).
  const [searchRegionScale, setSearchRegionScale] = useState<'state' | 'city' | 'zip' | 'address' | null>(null);
  // The exact 2-letter code Google assigned the searched state (only ever
  // set for scale === 'state') -- lets a state search filter by the real
  // `state` column instead of just a lat/lng bounding box. Needed because
  // a bounding box is a rectangle, not the state's real shape: searching
  // Maine returned a few New Hampshire results sitting in the box's
  // corner even with zero added Radius buffer, since NH pokes into that
  // corner of Maine's own bounding rectangle.
  const [searchStateCode, setSearchStateCode] = useState<string | null>(null);
  // Whether a state search filters to exactly that state (the `state`
  // column, exact) vs. the bounding-box + Radius-buffer behavior (which
  // can include nearby out-of-state results, by design, for a visitor who
  // specifically wants to see just-over-the-border options). Defaults to
  // true: home care/hospice licensing and insurance reimbursement are
  // typically state-bound, so an out-of-state result showing up
  // unannounced is more likely to mislead than help.
  const [confineToState, setConfineToState] = useState(true);
  const [gettingLocation, setGettingLocation] = useState(false);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [suggestions, setSuggestions] = useState<google.maps.places.AutocompletePrediction[]>([]);
  // -1 means nothing highlighted -- the input's own typed text is still
  // "the value," matching how a real combobox distinguishes "browsing
  // options with arrow keys" from "just typing." Confirmed via an
  // accessibility audit that arrow keys previously did nothing at all
  // (no role="combobox"/listbox structure existed), a full ARIA
  // Authoring Practices combobox-pattern miss.
  const [highlightedIndex, setHighlightedIndex] = useState(-1);
  const autocompleteService = useRef<google.maps.places.AutocompleteService | null>(null);
  const suggestionsRef = useRef<HTMLDivElement>(null);
  // Set by whichever geocode call actually resolves the current location
  // (handleSelectSuggestion's own geocode, or handleSearch's) -- read at
  // submit time to decide whether a real boundary trace should even be
  // attempted (see shouldTraceBoundary below).
  const lastGeocodeScaleRef = useRef<'state' | 'city' | 'zip' | 'address' | undefined>(undefined);
  // The location text handleSearch last actually geocoded/fit the camera
  // to -- lets a re-click of Search with the SAME text (just refining
  // Type of Care/Radius and clicking Search again instead of waiting for
  // the debounced auto-refresh) skip the camera re-fit entirely, instead
  // of treating every click as a brand new search. See handleSearch.
  const lastSearchedLocationRef = useRef<string | null>(null);
  // Shows/hides the licensing/insurance note next to "Confine to state
  // only" -- a hover-or-tap popover (not a native title tooltip, which
  // doesn't work on touch) explaining why crossing state lines matters.
  const [showStateInsuranceInfo, setShowStateInsuranceInfo] = useState(false);

  // Restores a search coming in from the URL (e.g. hitting the browser
  // back button after clicking into a provider page). Checks the results
  // cache first -- if this exact search (location + care types) was run
  // recently, reuse its results/coordinates directly instead of visibly
  // reloading/repopulating the map on every back navigation. Falls back
  // to a real geocode + fetch (same as a fresh submit) if there's no
  // cache hit, e.g. the cache expired or this is a bookmarked/shared URL.
  // Runs once on mount only; handleSearch is defined later in this
  // component, but that's fine here -- effect callbacks aren't invoked
  // until after the full render (and every const in it) has already
  // executed.
  useEffect(() => {
    if (!initialLocationParam) return;

    const cached = loadSearchCache(initialLocationParam, selectedServices);
    if (cached) {
      setLocations(cached.results);
      setUserCoords(cached.userCoords);
      setSearchBounds(cached.searchBounds);
      // Also restore what a real submit would have set -- without these, a
      // restored state search's searchRegionScale came back null, which
      // broke the "Radius is a mile buffer around the state" behavior on
      // the very next refinement (it silently fell back to treating Radius
      // as a no-op instead).
      setSearchRegionScale(cached.searchRegionScale);
      setDistanceRadius(cached.distanceRadius);
      lastGeocodeScaleRef.current = cached.searchRegionScale ?? undefined;
      return;
    }

    handleSearch();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Restores the scroll position captured just before navigating to a
  // provider page (see ProviderCard.tsx / MapSearch.tsx), once results
  // have actually finished loading. Restoring any earlier -- e.g. on the
  // native browser attempt this deliberately overrides via
  // scrollRestoration = 'manual' above -- would land on a position that
  // doesn't exist yet on the still-short, not-yet-repopulated page.
  useEffect(() => {
    if (loading) return;
    const saved = sessionStorage.getItem(HOME_SCROLL_STORAGE_KEY);
    if (!saved) return;
    sessionStorage.removeItem(HOME_SCROLL_STORAGE_KEY);
    requestAnimationFrame(() => {
      window.scrollTo({ top: Number(saved), behavior: 'auto' });
    });
  }, [loading]);

  // Mirrors the current search into the URL's query string (via
  // replaceState, not pushState -- refining filters shouldn't spam the
  // back button with an entry per change) so it survives the full page
  // reload that happens when navigating back from a provider page (see
  // the restore effect above).
  useEffect(() => {
    const params = new URLSearchParams();
    if (hasSubmittedSearch) {
      if (location.trim()) params.set('location', location);
      if (selectedServices.length > 0) params.set('care', selectedServices.join(','));
      if (distanceRadius !== 999999) params.set('radius', String(distanceRadius));
      if (viewMode !== 'map') params.set('view', viewMode);
    }
    const query = params.toString();
    const newUrl = `${window.location.pathname}${query ? `?${query}` : ''}`;
    if (newUrl !== `${window.location.pathname}${window.location.search}`) {
      window.history.replaceState({}, '', newUrl);
    }
  }, [hasSubmittedSearch, location, selectedServices, distanceRadius, viewMode]);

  // Mirrors the latest render's values so the debounced effect below can
  // read current data from inside its setTimeout callback instead of
  // whatever its closure captured when the effect last ran. Needed because
  // that effect deliberately only re-fires on selectedServices changing
  // (see its own comment) -- on a URL-seeded initial load (a bookmark, or
  // Back navigation from a provider page), hasSubmittedSearch is already
  // true on the very first render, but userCoords/searchBounds are still
  // their default null right up until the cache-hit effect's setters land
  // a render later. Without this, the debounced effect's original closure
  // (frozen at that first render) would still see the stale nulls 400ms
  // later and silently overwrite the just-restored results with an empty,
  // coords-less query -- confirmed via a live repro of the Back-navigation
  // flow.
  const latestSearchStateRef = useRef({
    hasSubmittedSearch,
    searchRegionScale,
    searchBounds,
    distanceRadius,
    location,
    selectedServices,
    userCoords,
    searchStateCode,
    confineToState,
  });
  useEffect(() => {
    latestSearchStateRef.current = {
      hasSubmittedSearch,
      searchRegionScale,
      searchBounds,
      distanceRadius,
      location,
      selectedServices,
      userCoords,
      searchStateCode,
      confineToState,
    };
  });

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
      const current = latestSearchStateRef.current;
      if (!current.hasSubmittedSearch) return;
      // Re-expand for a state search -- see the matching comment in
      // handleSearch for why a state uses a mile buffer around its shape
      // instead of centroid-distance filtering.
      const queryBounds =
        current.searchRegionScale === 'state' && current.searchBounds
          ? expandBoundsByMiles(current.searchBounds, current.distanceRadius)
          : current.searchBounds;
      loadLocations(current.location, current.selectedServices, current.userCoords, queryBounds, current.searchRegionScale, current.distanceRadius, current.searchStateCode, current.confineToState);
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
        setHighlightedIndex(-1);
        return;
      }

      try {
        const request = {
          input: location,
          componentRestrictions: { country: 'us' }
        };

        autocompleteService.current.getPlacePredictions(request, (predictions, status) => {
          const realSuggestions =
            status === google.maps.places.PlacesServiceStatus.OK && predictions ? predictions.slice(0, 5) : [];

          // Surfaces "[State] (entire state)" as its own distinct, always-
          // first option while typing a state's name -- Google's own
          // Places Autocomplete tends to bury or omit the bare state in
          // favor of cities that share part of its name (confirmed live:
          // typing "New York" doesn't offer the state as a suggestion at
          // all, only NYC and other New York-named places), which is
          // exactly what made "how do I even search for the whole state"
          // a real question. Only added for the CURRENT text (not stale
          // once it's already picked), and only when the match is
          // unambiguous (see findUniqueStateMatch).
          const stateMatch = findUniqueStateMatch(location);
          let suggestionsWithState = realSuggestions;
          if (stateMatch) {
            // Drops any real Google suggestion that's ALSO just this same
            // state (e.g. Google occasionally offers "New York, USA"
            // itself as one of its own predictions) so the state doesn't
            // show up twice, once as the synthetic entry and once as
            // Google's own -- the synthetic one is kept since it's the
            // one guaranteed to resolve correctly (see
            // resolveDirectStateMatch/handleSelectSuggestion).
            const withoutDuplicateState = realSuggestions.filter(
              (s) => resolveStateCode(s.description.split(',')[0].trim()) !== stateMatch.code
            );
            suggestionsWithState = [
              {
                place_id: `${DIRECT_STATE_PLACE_ID_PREFIX}${stateMatch.code}`,
                description: `${stateMatch.name} (entire state)`,
              } as google.maps.places.AutocompletePrediction,
              ...withoutDuplicateState,
            ];
          }

          setSuggestions(suggestionsWithState.slice(0, 5));
          setHighlightedIndex(-1);
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

  const loadLocations = async (
    searchText: string,
    services: string[],
    // Drives the actual geographic query below (not read from
    // userCoords/searchBounds state directly, since callers that just
    // geocoded call setUserCoords/setSearchBounds and then this in the
    // same synchronous tick, before those state updates have actually
    // landed -- reading state here would query/cache against the
    // previous search's coordinates) -- and also cached alongside the
    // results for instant restore on back-navigation (see
    // saveSearchCache below).
    coords: { lat: number; lng: number } | null = null,
    bounds: { south: number; west: number; north: number; east: number } | null = null,
    // Same same-tick-staleness reasoning as coords/bounds above -- read as
    // params (defaulting to current state for callers where that's already
    // fresh enough, like the debounced refinement effect) rather than
    // closing over searchRegionScale/distanceRadius directly, so a call
    // from inside handleSearch's own geocode branch caches the region
    // scale/radius it just resolved, not whatever was left over from the
    // previous search.
    scale: 'state' | 'city' | 'zip' | 'address' | null = searchRegionScale,
    radius: number = distanceRadius,
    stateCode: string | null = searchStateCode,
    confine: boolean = confineToState
  ) => {
    if (services.length === 0) {
      // No care type checked -- show nothing rather than dropping the
      // service filter entirely, which would silently surface older,
      // out-of-scope data that happens to share the same publish flags.
      setLocations([]);
      setAutoExpandNotice(null);
      return;
    }

    setLoading(true);
    try {
      // Extracted so the same query can be re-run against a wider box
      // below (auto-expand on zero results) without duplicating all of
      // this filtering logic.
      const runQuery = async (queryBounds: typeof bounds) => {
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
        const isBroadTerm = trimmed !== '' && BROAD_LOCATION_TERMS.has(trimmed.toLowerCase());

        if (!isBroadTerm) {
          if (scale === 'state' && confine && stateCode) {
            // Filters to exactly this state via the `state` column itself
            // rather than a lat/lng bounding box. A bounding box is a
            // rectangle, not the state's real shape -- confirmed live that
            // searching Maine included 3 New Hampshire results sitting in
            // the box's corner (NH pokes into that corner of Maine's own
            // bounding rectangle) even with zero added Radius buffer.
            // Filtering the column directly is exact regardless of the
            // state's shape, and sidesteps the antimeridian box-math below
            // entirely for the default (confined) case.
            query = query.eq('state', stateCode);
          } else if (queryBounds) {
            // A real geographic region -- a state's actual extent, or a box
            // expanded around a geocoded city/zip/address, sized to that
            // search's own scale (see computeRegionBounds/CITY_RADIUS_
            // DEGREES/ZIP_RADIUS_DEGREES/ADDRESS_RADIUS_DEGREES below,
            // where `bounds` gets computed in handleSearch/
            // handleSelectSuggestion). Querying by the real geography
            // instead of text-matching the searched name is what lets
            // "Chicago, IL" include Evanston, Oak Park, Naperville -- real
            // nearby suburbs that don't happen to contain "Chicago" in
            // their name -- instead of only the literal city-name match.
            query = query
              .gte('latitude', queryBounds.south)
              .lte('latitude', queryBounds.north);

            // Alaska (and any other region whose real extent crosses the
            // antimeridian) has a bounding box where west is a large
            // POSITIVE longitude (~172, the Aleutians going the short way)
            // and east is a large NEGATIVE one (~-130, the panhandle) --
            // confirmed live geocoding "Alaska": southwest.lng=172.35,
            // northeast.lng=-129.97. A plain gte(west).lte(east) then reads
            // as "longitude >= 172 AND longitude <= -130", which no real
            // longitude satisfies -- every Alaska search returned zero
            // rows. When west > east, the box actually wraps around 180/
            // -180, so a point is inside it if its longitude is on EITHER
            // side (>= west OR <= east), not both.
            query = queryBounds.west > queryBounds.east
              ? query.or(`longitude.gte.${queryBounds.west},longitude.lte.${queryBounds.east}`)
              : query.gte('longitude', queryBounds.west).lte('longitude', queryBounds.east);
          } else if (coords) {
            // A point with no defined region (GPS "My Location", or a
            // geocode result with no bounds at all) -- the city-scale box,
            // centered on the point instead of a search-provided region.
            query = query
              .gte('latitude', coords.lat - CITY_RADIUS_DEGREES)
              .lte('latitude', coords.lat + CITY_RADIUS_DEGREES)
              .gte('longitude', coords.lng - CITY_RADIUS_DEGREES)
              .lte('longitude', coords.lng + CITY_RADIUS_DEGREES);
          } else if (trimmed) {
            // Fallback for when geocoding hasn't resolved (or failed) --
            // the original text-based city/state/zip matching. Google
            // Places descriptions (and plenty of manually-typed searches)
            // are comma-separated -- "Anchorage, AK, USA", "Alaska, USA",
            // "99501, USA", "123 Main St, Anchorage, AK 99501, USA".
            // Splitting on commas and matching each part against
            // city/state/zip (instead of the whole string as one
            // substring) means any of those forms works, not just a bare
            // city name. A trailing country part is dropped so it can't
            // suppress otherwise-valid matches.
            const parts = trimmed
              .split(',')
              .map((p) => p.trim())
              .filter((p) => p && !BROAD_LOCATION_TERMS.has(p.toLowerCase()));

            // Recognized state-code parts and everything else are collected
            // separately and applied as two independent .or() calls rather
            // than one combined list -- supabase-js's .or() appends a
            // separate `or=` query param each time it's called (confirmed
            // by reading postgrest-js's source), and PostgREST ANDs
            // separate top-level params together (confirmed live against
            // Supabase). That's what makes "Portland, ME" require BOTH a
            // Maine state match AND a Portland city/zip match, instead of
            // every Portland nationwide unioned with every Maine location.
            const stateOrParts: string[] = [];
            const otherOrParts: string[] = [];
            for (const rawPart of parts.length > 0 ? parts : [trimmed]) {
              const part = rawPart.replace(/[%]/g, '');
              if (!part) continue;

              // The `state` column only ever stores 2-letter codes ("AK"),
              // so a typed full name ("Alaska") has to be translated first
              // or it can never match. resolveStateCode also accepts an
              // already-valid code.
              const stateCode = resolveStateCode(part);
              if (stateCode) {
                // A part that resolves to a real state code is almost
                // certainly the state field of a structured address, not a
                // city/zip fragment -- also ILIKE-matching it against
                // city/postal_code as a generic 2-letter substring produces
                // a flood of false positives (e.g. "ME" matches any city
                // containing "me": Fremont, Sacramento, Yakima,
                // Somerville...). Only the state match applies here.
                stateOrParts.push(`state.ilike.%${stateCode}%`);
              } else {
                otherOrParts.push(`city.ilike.%${part}%`);
                otherOrParts.push(`postal_code.ilike.%${part}%`);
                otherOrParts.push(`state.ilike.%${part}%`);
              }
            }

            if (stateOrParts.length > 0) {
              query = query.or(stateOrParts.join(','));
            }
            if (otherOrParts.length > 0) {
              query = query.or(otherOrParts.join(','));
            }
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

        // Supabase/PostgREST caps any single response at 1000 rows by
        // default regardless of what .limit() asks for -- confirmed live
        // that a California search (2,714 real matches for the 3 MVP
        // service types) silently truncated to exactly 1000. Paginating
        // with .range() in PAGE_SIZE chunks up to RESULTS_LIMIT gets the
        // real total for a search this large; the vast majority of
        // searches are well under 1000 and finish after one page.
        const PAGE_SIZE = 1000;
        let data: any[] = [];
        let offset = 0;
        while (data.length < RESULTS_LIMIT) {
          const { data: page, error } = await query.range(offset, offset + PAGE_SIZE - 1);
          if (error) throw error;
          if (!page || page.length === 0) break;
          data = data.concat(page);
          if (page.length < PAGE_SIZE) break;
          offset += PAGE_SIZE;
        }

        return (
          data?.map((loc: any) => ({
            ...loc,
            service_types: loc.service_types?.map((st: any) => st.service_type).filter(Boolean) || []
          })) || []
        );
      };

      let mapped = await runQuery(bounds);

      if (import.meta.env.DEV) {
        console.log('[loadLocations] initial query', { searchText, services, coords, bounds, resultCount: mapped.length });
      }

      // Auto-expand: a bounds-based search (not the rare text-only
      // fallback, which has no defined box to widen) that comes back with
      // literally nothing gets progressively wider retries instead of
      // leaving a blank map -- most useful for a ZIP/address search,
      // whose starting box is only a few miles across and can genuinely
      // have zero accredited providers in it even though real results
      // exist nearby. Whichever step actually finds something also
      // becomes the new Radius selector value, so the UI honestly
      // reflects what's actually being shown rather than a silent
      // widening.
      // Tracks whichever box actually produced results -- bounds itself
      // stays the original (tight) box the caller passed in, so caching
      // against that instead of usedBounds would save a box known to
      // return zero results, defeating the point of caching for an instant
      // restore.
      let usedBounds = bounds;
      let usedRadius = radius;
      let expandedNotice: string | null = null;
      if (mapped.length === 0 && coords && bounds) {
        for (const step of RADIUS_EXPANSION_STEPS) {
          const wider = expandBoundsByMiles(bounds, step);
          const retryResults = await runQuery(wider);
          if (import.meta.env.DEV) {
            console.log('[loadLocations] expansion retry', { step, wider, resultCount: retryResults.length });
          }
          if (retryResults.length > 0) {
            mapped = retryResults;
            usedBounds = wider;
            usedRadius = step;
            setDistanceRadius(step);
            expandedNotice = `No providers found in the immediate area -- showing results within ${step} miles instead.`;
            break;
          }
        }
      }
      setAutoExpandNotice(expandedNotice);

      setLocations(mapped);
      saveSearchCache(searchText, services, mapped, coords, usedBounds, scale, usedRadius);
    } catch (error) {
      // Previously left `locations` at whatever it already held (stale
      // results from a prior search, or the initial empty array) with no
      // visible indication anything went wrong -- a query error (e.g. a
      // malformed filter) looked identical to "search legitimately found
      // nothing."
      console.error('Error loading locations:', error);
      setLocations([]);
      setAutoExpandNotice(null);
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
  // Some, but not all, of the 3 real checkboxes are checked -- "indeterminate"
  // is a DOM property, not an HTML attribute, so it has to be set imperatively
  // via a ref rather than declaratively like `checked`. Without this, "All
  // Care" silently reported as fully UNCHECKED (checked=false) the moment a
  // visitor unchecked just one of the three real options, even though 2 of 3
  // service types were still actively being searched -- misleading for a
  // sighted user glancing at an empty box, and outright wrong for a screen
  // reader user who'd hear "not checked" while services were still selected.
  const someServicesSelected = selectedServices.length > 0 && !allServicesSelected;
  const allCareCheckboxRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (allCareCheckboxRef.current) {
      allCareCheckboxRef.current.indeterminate = someServicesSelected;
    }
  }, [someServicesSelected]);

  const toggleAllServices = () => {
    setSelectedServices(allServicesSelected ? [] : allServiceSlugs);
  };

  // Submitting with zero care types checked used to search zero services
  // (by design -- see loadLocations), which reads as "the search is
  // broken" rather than "you forgot to pick something." Since a location
  // search with no care type filter at all is a perfectly reasonable
  // thing to want, submitting now falls back to searching every service
  // instead -- and visibly checks those boxes, so the UI honestly
  // reflects what's actually being searched rather than a silent
  // fallback. Returns the effective list synchronously (setSelectedServices
  // won't be reflected until the next render) for immediate use in the
  // same submit.
  const ensureServicesSelected = (): string[] => {
    if (selectedServices.length > 0) return selectedServices;
    setSelectedServices(allServiceSlugs);
    return allServiceSlugs;
  };

  // Shared between the centered hero card (compact) and the post-search
  // filters row (full-size) so Type of Care can be picked before the very
  // first search, not just to refine an already-populated map.
  // Stacked, not a flat row -- "All Care" sits above a divider with the
  // three real options indented beneath it, so it reads as a master
  // toggle over sub-options rather than a fourth sibling choice (a thin
  // vertical divider in a single row wasn't enough of a visual cue).
  const renderCareTypeCheckboxes = (compact: boolean) => (
    <div className={`border-2 border-neutral-300 rounded-xl bg-white ${compact ? 'p-1.5' : 'p-2'}`}>
      <label className="flex items-center gap-1.5 cursor-pointer px-1.5 py-1">
        <input
          ref={allCareCheckboxRef}
          type="checkbox"
          checked={allServicesSelected}
          onChange={toggleAllServices}
          aria-checked={someServicesSelected ? 'mixed' : allServicesSelected}
          className="w-4 h-4 rounded border-neutral-300 text-primary-500 focus:ring-2 focus:ring-primary-200"
        />
        <span className={`font-semibold text-neutral-900 ${compact ? 'text-xs' : 'text-sm'}`}>All Care</span>
      </label>
      <div className="mt-1 pt-1.5 border-t border-neutral-200 pl-5 space-y-1">
        {allServiceSlugs.map((slug) => (
          <label key={slug} className="flex items-center gap-1.5 cursor-pointer px-1.5 py-0.5">
            <input
              type="checkbox"
              checked={selectedServices.includes(slug)}
              onChange={() => toggleService(slug)}
              className="w-3.5 h-3.5 rounded border-neutral-300 text-primary-500 focus:ring-2 focus:ring-primary-200"
            />
            <span className={`text-neutral-700 ${compact ? 'text-xs' : 'text-sm'}`}>{ALL_SERVICES[slug]}</span>
          </label>
        ))}
      </div>
    </div>
  );

  // Shared by handleSearch's direct-state short-circuit and the synthetic
  // "[State] (entire state)" suggestion below -- resolves a search string
  // to a state's real bounds/center straight from the pre-fetched
  // boundary bundle (see boundaryLookup.ts), with no geocoding call and no
  // risk of Google's city/state ambiguity for the name (confirmed live:
  // "New York, USA" geocodes to the city, not the state -- see the fuller
  // explanation in handleSearch). Only the first comma-part is checked
  // ("New York, USA" -> "New York"), so a real city/address search that
  // happens to start differently never matches by accident. Returns null
  // for anything that isn't a recognizable, bundled state name.
  const resolveDirectStateMatch = async (
    searchText: string
  ): Promise<{
    stateCode: string;
    bounds: { south: number; west: number; north: number; east: number };
    coords: { lat: number; lng: number };
    boundary: BoundaryGeoJSON;
  } | null> => {
    // Strips the synthetic "[State] (entire state)" suggestion's own
    // suffix (see DIRECT_STATE_PLACE_ID_PREFIX below) before resolving --
    // without this, resolveStateCode never matched "New York (entire
    // state)" against "new york", silently returned null, and every
    // direct-state-match selection fell through to Google's geocoder
    // instead, which (confirmed live) resolves multi-word state names
    // like "New York" to the CITY rather than the state -- the exact
    // same city/state ambiguity this direct-match path exists to avoid
    // in the first place. Single-word states (Texas, Alaska) happened to
    // still geocode to something reasonable via that fallback, which is
    // why this went unnoticed until a state search that specifically
    // needed the real state shape (not just a plausible-looking point)
    // was checked closely.
    const withoutSuffix = searchText.replace(/\s*\(entire state\)\s*$/i, '');
    const stateCode = resolveStateCode(withoutSuffix.split(',')[0].trim());
    if (!stateCode) return null;
    const boundary = await fetchStateBoundary(stateCode);
    if (!boundary) return null;
    const bounds = computeBoundsFromGeoJSON(boundary);
    const coords = computeCenterFromBounds(bounds);
    return { stateCode, bounds, coords, boundary };
  };

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

    // The synthetic "[State] (entire state)" suggestion (see the
    // suggestions list below) -- a real Google place_id never has this
    // prefix, so this can't collide with an actual suggestion.
    if (placeId.startsWith(DIRECT_STATE_PLACE_ID_PREFIX)) {
      const match = await resolveDirectStateMatch(description);
      if (match) {
        setUserCoords(match.coords);
        setSearchRegionScale('state');
        setSearchStateCode(match.stateCode);
        setSearchBounds(match.bounds);
        setDistanceRadius(suggestDefaultRadius('state'));
        lastGeocodeScaleRef.current = 'state';
      }
      return;
    }

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
          const rawBounds = box
            ? {
                south: box.getSouthWest().lat(),
                west: box.getSouthWest().lng(),
                north: box.getNorthEast().lat(),
                east: box.getNorthEast().lng(),
              }
            : null;
          const scale = classifyRegionScale(results[0].types);
          setSearchBounds(computeRegionBounds(coords, scale, rawBounds));
          setDistanceRadius(suggestDefaultRadius(scale));
          setSearchRegionScale(scale);
          setSearchStateCode(scale === 'state' ? extractStateCode(results[0].address_components) : null);
          lastGeocodeScaleRef.current = scale;
        }
      });
    }
  };

  // Single source of truth for "how specific is this search," everything
  // below derives from this instead of each repeating its own types
  // checks.
  const classifyRegionScale = (types: string[] | undefined): 'state' | 'city' | 'zip' | 'address' => {
    if (types?.includes('administrative_area_level_1')) return 'state';
    if (types?.includes('postal_code')) return 'zip';
    if (
      types?.includes('street_address') ||
      types?.includes('premise') ||
      types?.includes('subpremise') ||
      // A named landmark/business (confirmed live: "Willis Tower" typed
      // and searched directly, not picked from the autocomplete dropdown,
      // comes back from Google's REST geocoding endpoint as only
      // ['establishment', 'point_of_interest', 'tourist_attraction'] --
      // no street_address/premise at all -- so it fell through to the
      // generic 'city' bucket (25mi radius, no radius circle) despite
      // resolving to a precise single point ("233 S Wacker Dr, Chicago,
      // IL 60606, USA"). A landmark/POI is exactly as precise a point as
      // a street address for this purpose.
      types?.includes('point_of_interest') ||
      types?.includes('establishment')
    ) return 'address';
    return 'city';
  };

  // Decides how much geography a geocoded result should actually cover.
  // A state or ZIP gets Google's own real extent -- verified live:
  // geocoding "60640" returns a real bounds box (~2mi x 2.5mi), Google's
  // actual definition of that ZIP's area, not an arbitrary radius.
  // ZIP_RADIUS_DEGREES is only a fallback for the rare case a postal_code
  // result has no bounds at all. A city/locality result's own bounds are
  // typically just the city limits (too narrow -- CITY_RADIUS_DEGREES
  // deliberately widens past that to include real nearby suburbs), and a
  // street address's viewport is a tiny "reasonable to display" box
  // around a single point (verified: ~0.2mi, far too small to be
  // useful) -- so those two intentionally use a fixed expansion instead
  // of Google's raw bounds. Always returns a real region, never null,
  // since even an exact-address search should still show what's nearby.
  const computeRegionBounds = (
    coords: { lat: number; lng: number },
    scale: 'state' | 'city' | 'zip' | 'address',
    rawBounds: { south: number; west: number; north: number; east: number } | null
  ): { south: number; west: number; north: number; east: number } => {
    if (rawBounds && (scale === 'state' || scale === 'zip')) {
      return rawBounds;
    }

    const radius = scale === 'zip' ? ZIP_RADIUS_DEGREES : scale === 'address' ? ADDRESS_RADIUS_DEGREES : CITY_RADIUS_DEGREES;

    return {
      south: coords.lat - radius,
      north: coords.lat + radius,
      west: coords.lng - radius,
      east: coords.lng + radius,
    };
  };

  // A sensible starting point for the Radius selector, matched to how
  // specific the search actually is -- explicitly a suggestion the
  // visitor can change, not a hard constraint (the "Radius" label below
  // notes this). A ZIP code search landing on the same "Any distance"
  // default as a state search meant the Radius control did nothing
  // useful until manually touched. For a state, this is the mile buffer
  // added around the state's own shape (see expandBoundsByMiles), not a
  // centroid-distance radius.
  //
  // State defaults to 999999 ("Any distance", which for state scale means
  // no added buffer at all -- see expandBoundsByMiles -- i.e. exactly the
  // state's own boundary) rather than a mile buffer past the state line.
  // Home care/hospice licensing and insurance reimbursement are typically
  // state-bound, so an out-of-state provider a few miles past the border
  // may not actually be able to serve a visitor even though it looks
  // nearby on the map -- defaulting to the state itself avoids surfacing
  // those by default. Still just a suggestion: a visitor who specifically
  // wants to see just-over-the-border options can widen it manually.
  const suggestDefaultRadius = (scale: 'state' | 'city' | 'zip' | 'address'): number => {
    if (scale === 'state') return 999999;
    if (scale === 'zip' || scale === 'address') return 5;
    return 25; // city
  };

  // A real traced boundary is only worth attempting for a city or state --
  // Nominatim doesn't reliably have ZIP/address-level polygons anyway,
  // and (confirmed live) a ZIP search's compound text ("Chicago, IL
  // 60640, USA") gets resolved by Nominatim to the city as its best
  // match -- a real boundary, just the wrong scope for what was actually
  // searched. ZIP/address searches already have an accurate outline from
  // Google's own bounds (see computeRegionBounds), so skip the
  // mismatched trace entirely rather than show it.
  const shouldTraceBoundary = (scale: 'state' | 'city' | 'zip' | 'address'): boolean =>
    scale === 'state' || scale === 'city';

  const handleSearch = async () => {
    setShowSuggestions(false);
    if (!location.trim()) return;

    // Re-clicking Search with the exact same location text as last time
    // (refining Type of Care/Radius and hitting Search again, rather than
    // waiting for the debounced auto-refresh) isn't a new search --
    // confirmed live this was resetting the camera to the default fit
    // every time, discarding any manual pan/zoom, even though nothing
    // about WHERE was searched actually changed. Route it through the
    // same refinement path Radius/"Confine to state" already use instead
    // of the full new-search flow below.
    if (
      hasSubmittedSearch &&
      userCoords !== null &&
      lastSearchedLocationRef.current !== null &&
      location.trim().toLowerCase() === lastSearchedLocationRef.current.toLowerCase()
    ) {
      const queryBounds =
        searchRegionScale === 'state' && searchBounds
          ? expandBoundsByMiles(searchBounds, distanceRadius)
          : searchBounds;
      loadLocations(location, ensureServicesSelected(), userCoords, queryBounds, searchRegionScale, distanceRadius, searchStateCode, confineToState);
      return;
    }

    setHasSubmittedSearch(true);
    setSearchGeneration((n) => n + 1);
    lastSearchedLocationRef.current = location.trim();

    // Cleared immediately, not left showing until the new results land.
    // Two reasons: visually, the previous search's pins stayed on screen
    // (reading as "did my new search even register?") for however long the
    // geocode+query took; more importantly, MapSearch's fitBounds effect
    // treats "there are markers" as "the current search's markers are
    // ready to fit to" -- with the old markers still present the instant
    // searchGeneration/searchBounds change, it would fit to (and then lock
    // onto, for this generation) the PREVIOUS search's pins before the new
    // ones ever arrived, which is why the camera sometimes never
    // re-zoomed to the new search at all. This intentionally does NOT
    // apply to a same-search refinement (Type of Care, Radius) -- those
    // still keep prior results visible during their refetch (see
    // loadLocations's callers below, none of which clear locations first).
    setLocations([]);
    setLoading(true);
    setAutoExpandNotice(null);

    // Cleared immediately (not just left to be overwritten once the fetch
    // below resolves) so a stale outline from the previous search doesn't
    // linger on screen while this one is in flight.
    setBoundaryPolygon(null);

    // Tracked locally rather than read back from userCoords/searchBounds
    // state below -- setUserCoords/setSearchBounds won't have landed by
    // the time loadLocations runs in this same synchronous flow, so
    // reading state here would cache the previous search's coordinates.
    let effectiveCoords = userCoords;
    // The RAW region (unexpanded) -- what the outline/map-fit displays.
    let effectiveDisplayBounds = searchBounds;
    // Whichever geocode actually ran resolves this -- handleSelectSuggestion's
    // own geocode if a suggestion was picked (userCoords already set, so
    // this function's own geocode below is skipped entirely), or the
    // geocode just below otherwise.
    let effectiveScale = lastGeocodeScaleRef.current;
    let effectiveRadius = distanceRadius;
    let effectiveStateCode = searchStateCode;

    // Set true when the direct-state-name short-circuit just below already
    // resolved the boundary polygon, so the separate boundary-trace block
    // further down doesn't redundantly re-fetch the same thing.
    let boundaryAlreadyResolved = false;

    // A search that's exactly (or almost exactly) a US state's name --
    // "New York, USA", "Illinois" -- resolves correctly against Google's
    // geocoder only some of the time: confirmed live that "New York, USA"
    // geocodes to New York City (types: ['locality'], no state-level
    // result offered as an alternative), not the state, because the city
    // is the more "prominent" result Google returns for that text.
    // Checking known state names first (via resolveDirectStateMatch, using
    // the boundary polygon already bundled for all 50 states -- see
    // boundaryLookup.ts) sidesteps that ambiguity entirely for this common
    // case -- no geocoding call needed, and no risk of landing on the
    // wrong entity.
    const directMatch = !userCoords ? await resolveDirectStateMatch(location) : null;

    if (directMatch) {
      setUserCoords(directMatch.coords);
      effectiveCoords = directMatch.coords;
      setSearchRegionScale('state');
      effectiveScale = 'state';
      setSearchStateCode(directMatch.stateCode);
      effectiveStateCode = directMatch.stateCode;
      setSearchBounds(directMatch.bounds);
      effectiveDisplayBounds = directMatch.bounds;
      const suggestedRadius = suggestDefaultRadius('state');
      setDistanceRadius(suggestedRadius);
      effectiveRadius = suggestedRadius;
      lastGeocodeScaleRef.current = 'state';
      setBoundaryPolygon(directMatch.boundary);
      boundaryAlreadyResolved = true;
      if (import.meta.env.DEV) {
        console.log('[handleSearch] direct state match', { location, directMatch });
      }
    } else if (!userCoords) {
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
          effectiveCoords = coords;

          const scale = classifyRegionScale(result.types);
          effectiveScale = scale;
          setSearchRegionScale(scale);

          const stateCode = scale === 'state' ? extractStateCode(result.address_components) : null;
          effectiveStateCode = stateCode;
          setSearchStateCode(stateCode);

          const box = result.geometry.bounds || result.geometry.viewport;
          const rawBounds = box
            ? {
                south: box.southwest.lat,
                west: box.southwest.lng,
                north: box.northeast.lat,
                east: box.northeast.lng,
              }
            : null;
          const bounds = computeRegionBounds(coords, scale, rawBounds);
          setSearchBounds(bounds);
          effectiveDisplayBounds = bounds;

          const suggestedRadius = suggestDefaultRadius(scale);
          setDistanceRadius(suggestedRadius);
          effectiveRadius = suggestedRadius;

          // Keeps lastGeocodeScaleRef in sync with whichever geocode path
          // actually ran -- handleSelectSuggestion already writes this for
          // its own geocode, but this inline fallback (typed text, Enter,
          // no suggestion picked) never did, so a later call that reads the
          // ref (see the top of this function) could see a stale scale left
          // over from a previous, different search.
          lastGeocodeScaleRef.current = scale;

          if (import.meta.env.DEV) {
            console.log('[handleSearch] geocode OK', { location, scale, coords, bounds, suggestedRadius });
          }
        } else {
          // A non-OK status (REQUEST_DENIED, ZERO_RESULTS, OVER_QUERY_LIMIT,
          // INVALID_REQUEST...) previously fell through completely silently
          // here -- effectiveCoords/effectiveDisplayBounds/effectiveScale
          // were left at whatever they already were (null coords/bounds,
          // since this branch only runs when userCoords is already null;
          // but effectiveScale could still be a *stale* value left over in
          // lastGeocodeScaleRef from an earlier, different search). Logging
          // this is the only way to tell "geocode legitimately found
          // nothing" apart from "the query silently fell back to
          // ILIKE-text-matching with a leftover scale from last time."
          // ZERO_RESULTS here just means "nothing at that address" -- an
          // entirely normal outcome of a real user search, not a bug. Only
          // logging in DEV keeps the diagnostic value (see above) without
          // every ordinary no-results search reporting as a console error
          // in production, where it could trip error-monitoring tooling.
          if (import.meta.env.DEV) {
            console.error('Geocoding failed for search:', location, data.status, data.error_message);
          }
          effectiveScale = undefined;
          effectiveStateCode = null;
          lastGeocodeScaleRef.current = undefined;
        }
      } catch (error) {
        console.error('Geocoding error:', error);
        effectiveScale = undefined;
        effectiveStateCode = null;
        lastGeocodeScaleRef.current = undefined;
      }
    }

    if (import.meta.env.DEV) {
      console.log('[handleSearch] resolved', { location, effectiveScale, effectiveCoords, effectiveDisplayBounds, effectiveRadius });
    }

    // Only attempted for city/state-level searches -- a ZIP code or exact
    // address search already has an accurate outline from Google's own
    // bounds (see computeRegionBounds), and Nominatim doesn't reliably
    // have real ZIP/address-level boundary polygons anyway. Confirmed
    // live: searching "Chicago, IL 60640, USA" (a ZIP-level result) had
    // Nominatim resolve the whole compound string to Chicago the city --
    // a real boundary, just the wrong scope for what was actually
    // searched, tracing the city while the map zoomed to the ZIP. Fired
    // without awaiting either way -- best-effort visual extra, never
    // something the actual search should wait on.
    if (boundaryAlreadyResolved) {
      // Already set by the direct-state-name short-circuit above.
    } else if (effectiveScale === 'state' && effectiveStateCode) {
      // All 50 states are pre-fetched and bundled statically (see
      // boundaryLookup.ts) -- no live Nominatim round-trip, and no
      // loading window for the fallback rectangle to (not) show during,
      // for the one region type that's always one of a fixed, known set.
      // Falls back to the live lookup on a bundle miss (e.g. DC, or a
      // territory) rather than just giving up and showing nothing.
      setBoundaryLoading(true);
      fetchStateBoundary(effectiveStateCode).then((geo) => {
        if (geo) {
          setBoundaryPolygon(geo);
          setBoundaryLoading(false);
        } else {
          fetchSearchBoundary(location).then((liveGeo) => {
            setBoundaryPolygon(liveGeo);
            setBoundaryLoading(false);
          });
        }
      });
    } else if (effectiveScale && shouldTraceBoundary(effectiveScale)) {
      setBoundaryLoading(true);
      fetchSearchBoundary(location).then((geo) => {
        setBoundaryPolygon(geo);
        setBoundaryLoading(false);
      });
    } else {
      setBoundaryLoading(false);
    }

    // A state search's query bounds get expanded by the current Radius
    // (miles buffered around the state's actual shape -- see
    // expandBoundsByMiles) rather than filtered by centroid-distance
    // client-side the way city/zip/address searches are (see
    // filteredLocations below): a state is too large/irregularly shaped
    // for "distance from its centroid" to mean anything sensible --
    // Illinois's northern tip can be 200+ miles from the state's own
    // centroid despite being well inside the state.
    const queryBounds =
      effectiveScale === 'state' && effectiveDisplayBounds
        ? expandBoundsByMiles(effectiveDisplayBounds, effectiveRadius)
        : effectiveDisplayBounds;

    loadLocations(location, ensureServicesSelected(), effectiveCoords, queryBounds, effectiveScale ?? null, effectiveRadius, effectiveStateCode, confineToState);
  };

  // For a city/zip/address search, changing the Radius is purely a
  // client-side filter (see filteredLocations below) -- no need to
  // re-query, the candidate pool already covers it. A state search is
  // different: the Radius is a mile buffer added to the query's bounding
  // box itself (see expandBoundsByMiles), so widening/narrowing it
  // genuinely changes what should be fetched, and needs a real re-query.
  const handleRadiusChange = (newRadius: number) => {
    setDistanceRadius(newRadius);
    if (hasSubmittedSearch && searchRegionScale === 'state' && searchBounds) {
      loadLocations(location, selectedServices, userCoords, expandBoundsByMiles(searchBounds, newRadius), 'state', newRadius);
    }
  };

  // Toggling "Confine to just this state" needs a real re-query, not just
  // a client-side filter -- switching from the exact `state` column match
  // to the bounding-box+Radius query (or back) changes which rows the
  // database query itself returns, not just which of an already-fetched
  // set get shown.
  const handleConfineToStateChange = (confine: boolean) => {
    setConfineToState(confine);
    if (hasSubmittedSearch && searchRegionScale === 'state') {
      const queryBounds =
        !confine && searchBounds ? expandBoundsByMiles(searchBounds, distanceRadius) : searchBounds;
      loadLocations(location, selectedServices, userCoords, queryBounds, 'state', distanceRadius, searchStateCode, confine);
    }
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
        // close on "near me" instead of re-fitting a stale region. Scale
        // is set to 'address' (not a real geocode classification, just
        // reusing its precise-point treatment) so the Radius circle still
        // draws for "My Location" the way it always has.
        setSearchBounds(null);
        setSearchRegionScale('address');
        setHasSubmittedSearch(true);
        setSearchGeneration((n) => n + 1);
        // Same reasoning as handleSearch -- a genuinely new search (this is
        // one) shouldn't leave a previous search's markers up to be
        // mistakenly fit-and-locked-onto before this one's own results
        // arrive.
        setLocations([]);
        setLoading(true);
        setAutoExpandNotice(null);

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
            // Kept in sync with the text just set above so a later re-click
            // of Search (unchanged text) is recognized as a same-location
            // refinement instead of a new search -- see handleSearch.
            lastSearchedLocationRef.current = cityResult.formatted_address.trim();
            // Not delegating to handleSearch() here -- setLocation() above
            // hasn't committed yet by the time a synchronous call would
            // read `location` from this closure, so it would search with
            // the previous (likely empty) text. Using the just-resolved
            // address directly instead.
            loadLocations(cityResult.formatted_address, ensureServicesSelected(), coords, null, 'address', distanceRadius);
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
    // "Distance from userCoords" means "distance from the searched point"
    // everywhere except state scale, where userCoords is just a bounding-
    // box centroid/approximation, not a real point someone searched from
    // -- already not used for filtering/sorting a state search (see
    // filteredLocations below) for the same reason, and showing a raw
    // number here is actively misleading, not just unhelpful: confirmed
    // live that Hawaii's official state boundary includes the remote
    // Northwestern Hawaiian Islands, pulling that centroid far from the
    // populated islands, and Alaska's antimeridian-crossing shape can
    // do the same. Suppressing it here (instead of only at each display
    // site) is the single place that covers every card/popup/list that
    // shows a location's distance.
    if (userCoords && loc.latitude && loc.longitude && searchRegionScale !== 'state') {
      distance = calculateDistance(userCoords.lat, userCoords.lng, loc.latitude, loc.longitude);
    }
    return { ...loc, distance };
  });

  // Location text and service-type filtering already happened server-side
  // in loadLocations -- `locations` only ever contains matching rows, so
  // this just applies the distance radius (which needs the client-computed
  // distance above) and sorts.
  const filteredLocations = locationsWithDistance.filter((loc) => {
    // A state search's "distance from the searched point" is meaningless
    // (see the matching comment in handleSearch/expandBoundsByMiles) --
    // centroid-distance would wrongly exclude genuinely in-state results
    // far from the centroid. The query's bounding box is already
    // correctly expanded by the Radius for this case, so there's nothing
    // further to filter here.
    if (searchRegionScale === 'state') return true;

    if (distanceRadius < 999999 && loc.distance !== null) {
      if (loc.distance > distanceRadius) {
        return false;
      }
    }

    return true;
  }).sort((a, b) => {
    // Same reasoning -- distance-from-centroid isn't a meaningful order
    // for a state search, so leave the query's own order alone.
    if (searchRegionScale === 'state') return 0;

    // display_order was never a real column on locations (it belongs to
    // service_types) -- this always fell through to distance sorting in
    // practice. If per-location manual ordering is wanted later, it needs
    // a real column on locations or location_listing_settings first.
    if (a.distance === null && b.distance === null) return 0;
    if (a.distance === null) return 1;
    if (b.distance === null) return -1;
    return a.distance - b.distance;
  });

  // The search/filter panel -- a static column now, not an overlay. This
  // replaced a floating card that sat on top of the map/grid: it kept
  // fighting for the right size/position and, in grid view, covered the
  // actual results. A persistent side panel (the pattern Zillow/Redfin/
  // Realtor.com actually use on their results pages, as opposed to their
  // marketing homepages) sidesteps all of that -- it's a normal,
  // non-overlapping region, so there's no sizing/z-index tradeoff to make.
  const searchPanel = (
    <div className="bg-white rounded-2xl shadow-md border border-neutral-200 p-4">
      {/* Only shown once submitted -- "Where are you looking for care?"
          below already serves as the panel's heading before that point,
          so showing both together just repeated the same idea. This is
          purely the landmark heading for the post-search state, once that
          prompt disappears. neutral-600 measures 7.81:1 on white (AAA). */}
      {hasSubmittedSearch && (
        <h2 className="text-xs font-semibold text-neutral-600 uppercase tracking-wide mb-2">
          Find Care
        </h2>
      )}

      {!hasSubmittedSearch && (
        <div className="mb-3">
          <div className="flex items-center gap-2">
            <div className="w-9 h-9 bg-primary-100 rounded-full flex items-center justify-center flex-shrink-0">
              <MapPin className="w-4 h-4 text-primary-600" />
            </div>
            <p className="font-semibold text-navy-800 text-sm">Where are you looking for care?</p>
          </div>
          <p className="text-xs text-neutral-600 mt-0.5">Enter a city, state, address, or ZIP code.</p>
        </div>
      )}

      <div className="flex flex-col gap-2">
        <div className="relative">
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
              setBoundaryPolygon(null);
              setSearchRegionScale(null);
              setShowSuggestions(true);
              if (!value.trim()) {
                // Cleared back to empty -- fully reset to the idle prompt
                // state rather than leaving stale results showing behind
                // a panel that no longer reflects it.
                setHasSubmittedSearch(false);
              }
            }}
            onFocus={() => setShowSuggestions(true)}
            onKeyDown={(e) => {
              // Full ARIA Authoring Practices combobox pattern -- confirmed
              // via an accessibility audit that arrow keys previously did
              // nothing at all (no role="combobox"/listbox structure
              // existed, so Tab was the only way to reach a suggestion, with
              // zero programmatic relationship communicated to a screen
              // reader).
              if (e.key === 'ArrowDown' && showSuggestions && suggestions.length > 0) {
                e.preventDefault();
                setHighlightedIndex((i) => (i + 1) % suggestions.length);
              } else if (e.key === 'ArrowUp' && showSuggestions && suggestions.length > 0) {
                e.preventDefault();
                setHighlightedIndex((i) => (i - 1 + suggestions.length) % suggestions.length);
              } else if (e.key === 'Enter') {
                e.preventDefault();
                if (showSuggestions && highlightedIndex >= 0 && suggestions[highlightedIndex]) {
                  const s = suggestions[highlightedIndex];
                  handleSelectSuggestion(s.place_id, s.description);
                } else {
                  handleSearch();
                }
              } else if (e.key === 'Escape') {
                setShowSuggestions(false);
                setHighlightedIndex(-1);
              }
            }}
            placeholder="City, state, address, or ZIP"
            aria-label="Search location"
            role="combobox"
            aria-expanded={showSuggestions && suggestions.length > 0}
            aria-controls="location-suggestions-listbox"
            aria-autocomplete="list"
            aria-activedescendant={highlightedIndex >= 0 ? `location-suggestion-${highlightedIndex}` : undefined}
            className="w-full pl-9 pr-3 h-[48px] text-base border-2 border-neutral-300 rounded-lg focus:outline-none focus:ring-4 focus:ring-primary-200 focus:border-primary-500 transition-all"
            autoComplete="off"
          />
          {showSuggestions && suggestions.length > 0 && (
            // Was border-2 border-neutral-300 -- identical border weight/
            // color to the input field directly above it, which read as a
            // second input box rather than a dropdown menu (especially
            // with just one suggestion, confirmed live). A thinner,
            // lighter border plus the existing shadow reads more clearly
            // as a floating menu.
            <div
              ref={suggestionsRef}
              id="location-suggestions-listbox"
              role="listbox"
              className="absolute z-[10000] mt-1 w-full bg-white border border-neutral-200 rounded-xl shadow-2xl overflow-hidden"
            >
              {suggestions.map((suggestion, index) => (
                <button
                  key={suggestion.place_id}
                  id={`location-suggestion-${index}`}
                  role="option"
                  aria-selected={index === highlightedIndex}
                  onClick={() => handleSelectSuggestion(suggestion.place_id, suggestion.description)}
                  onMouseEnter={() => setHighlightedIndex(index)}
                  className={`w-full px-4 py-3 text-left transition-colors border-b border-neutral-100 last:border-b-0 flex items-center gap-3 ${
                    index === highlightedIndex ? 'bg-primary-50' : 'hover:bg-primary-50'
                  }`}
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
            className="btn-outline flex-1 px-3 h-[44px] flex items-center justify-center gap-1.5 whitespace-nowrap disabled:opacity-50 disabled:cursor-not-allowed"
            aria-label="Use my location"
            title="Use my location"
          >
            <Crosshair className={`w-4 h-4 ${gettingLocation ? 'animate-pulse' : ''}`} />
            <span className="text-sm">My Location</span>
          </button>

          <button
            onClick={handleSearch}
            className="btn-primary flex-1 px-3 h-[44px] flex items-center justify-center gap-1.5 whitespace-nowrap"
          >
            <Search className="w-4 h-4" />
            <span className="text-sm">Search</span>
          </button>
        </div>
      </div>

      {/* Type of Care lives here unconditionally (both before and after
          search) -- it's part of what's being searched for, not a
          refinement to bolt on afterward. */}
      <fieldset className="mt-3 border-0 p-0 m-0 min-w-0">
        <legend className="text-xs font-semibold text-navy-800 mb-1">Type of Care</legend>
        {renderCareTypeCheckboxes(true)}
      </fieldset>

      {hasSubmittedSearch && searchRegionScale === 'state' && (
        <div className="mt-3 animate-fade-in-up relative">
          <label className="flex items-center gap-1.5 cursor-pointer">
            <input
              type="checkbox"
              checked={confineToState}
              onChange={(e) => handleConfineToStateChange(e.target.checked)}
              className="w-3.5 h-3.5 rounded border-neutral-300 text-primary-500 focus:ring-2 focus:ring-primary-200"
            />
            <span className="text-xs font-semibold text-navy-800">
              Confine to {searchStateCode ? stateNameFromCode(searchStateCode) ?? 'this state' : 'this state'} only
            </span>
            <button
              type="button"
              // Was toggling on click -- since hover already opens it, a
              // natural hover-then-click motion immediately closed the
              // tooltip right as someone tried to read it (confirmed live).
              // Click now always opens (never closes) it instead; mouse
              // leaving is still what closes it, and click is what makes
              // it reachable at all on touch devices where hover never
              // fires.
              onClick={() => setShowStateInsuranceInfo(true)}
              onMouseEnter={() => setShowStateInsuranceInfo(true)}
              onMouseLeave={() => setShowStateInsuranceInfo(false)}
              className="text-neutral-500 hover:text-navy-800 focus:outline-none focus:ring-2 focus:ring-primary-200 rounded-full"
              aria-label="Why this matters"
              aria-expanded={showStateInsuranceInfo}
            >
              <HelpCircle className="w-3.5 h-3.5" />
            </button>
          </label>
          {showStateInsuranceInfo && (
            // navy-800 on neutral-50 measures 12.4:1 (AAA).
            <div
              role="tooltip"
              className="absolute z-20 mt-1.5 w-64 p-2.5 bg-neutral-50 border border-neutral-300 rounded-lg shadow-lg text-xs text-navy-800"
            >
              Home care, hospice, and similar licensing (including Medicare/
              Medicaid coverage) is usually specific to one state. A nearby
              out-of-state provider may not actually be able to serve you --
              worth confirming directly before choosing one.
            </div>
          )}
        </div>
      )}

      {hasSubmittedSearch && !(searchRegionScale === 'state' && confineToState) && (
        <div className="mt-3 animate-fade-in-up">
          <label htmlFor="radius-select" className="block text-xs font-semibold text-navy-800 mb-1">
            Radius <span className="font-normal text-neutral-600">(suggested -- change anytime)</span>
          </label>
          <select
            id="radius-select"
            value={distanceRadius}
            onChange={(e) => handleRadiusChange(Number(e.target.value))}
            className="w-full px-3 h-[38px] text-sm border-2 border-neutral-300 rounded-lg focus:outline-none focus:ring-4 focus:ring-primary-200 focus:border-primary-500 transition-all bg-white text-neutral-800 font-medium"
          >
            <option value={999999}>Any distance</option>
            <option value={5}>Within 5 mi</option>
            <option value={10}>Within 10 mi</option>
            <option value={25}>Within 25 mi</option>
            <option value={50}>Within 50 mi</option>
            <option value={100}>Within 100 mi</option>
          </select>
        </div>
      )}
    </div>
  );

  return (
    <section className="bg-neutral-50 py-6 md:py-10">
      <div className="max-w-7xl mx-auto px-3 sm:px-6 lg:px-8">
        {/* Was text-center across the full max-w-7xl width -- but the
            layout below it is an asymmetric two-column split (a fixed
            300px search panel + a much wider flexible map/grid column),
            so a heading centered on the FULL width doesn't align with
            either column or the gap between them, which read as "off"
            even though each piece was individually centered/aligned
            correctly. Left-aligning to the search panel's own edge gives
            the page one strong shared vertical line running from the
            heading straight down through the panel, instead of a
            centered block floating independently above an asymmetric
            grid. */}
        <div className="mb-4 md:mb-6">
          <h1 className="text-3xl md:text-4xl font-heading font-bold text-navy-800 mb-2">
            Find Accredited Care Providers
          </h1>

          <p className="text-base text-neutral-700 max-w-3xl leading-relaxed">
            Search accredited home care, hospice, and healthcare services in your area.
          </p>
        </div>

        <TrustStats />

        <div className="flex flex-col lg:flex-row gap-4">
          <div className="w-full lg:w-[300px] lg:flex-shrink-0">
            {/* top-24 (96px), not top-4 -- the site header is sticky at
                80px tall with z-50, which otherwise sits on top of (and
                covers) this panel once both are stuck simultaneously. */}
            <div className="lg:sticky lg:top-24">{searchPanel}</div>
          </div>

          {/* flex-1 column matches the panel's column exactly (own
              rounded-2xl/border/shadow card starting at the same y), so
              the count/toggle header lives INSIDE that card as its own
              top bar rather than floating above it -- that gap was what
              threw the two columns' top edges out of alignment before. */}
          <div className="flex-1 min-w-0">
            <div className="rounded-2xl overflow-hidden shadow-md border border-neutral-200 bg-white">
              {/* Same reasoning as the map/grid area below -- stays
                  mounted through a refinement's loading=true window
                  (previous results are still showing) instead of
                  flickering out and back in on every Type of Care
                  change. */}
              {hasSubmittedSearch && !(loading && filteredLocations.length === 0) && (
                <div className="flex items-center justify-between gap-3 px-4 py-2.5 border-b border-neutral-200 bg-neutral-50 animate-fade-in-up">
                  {/* aria-live so a screen reader user hears the updated
                      count after submitting a search or changing Type of
                      Care -- previously silent, so the only way to tell a
                      search had registered was visually re-reading the
                      page. polite (not assertive) so it doesn't interrupt
                      whatever the user is currently doing. */}
                  <p className="text-sm text-neutral-600" aria-live="polite">
                    {filteredLocations.length} accredited {filteredLocations.length === 1 ? 'provider' : 'providers'}
                    {location && ` near ${location}`}
                  </p>
                  {/* Was w-7/h-[34px] (~28x24px effective tap target on
                      mobile, measured live) -- below the ~44px comfortable
                      minimum for a primary, frequently-tapped control,
                      especially one likely used one-handed. Bumped to a
                      real 44px on mobile; sm: and up keep the original
                      auto-width-with-label sizing since desktop pointer
                      precision doesn't need it. */}
                  <div className="flex items-center gap-1.5 bg-white border border-neutral-200 rounded-lg p-1 h-11 sm:h-[34px] flex-shrink-0">
                    <button
                      onClick={() => setViewMode('map')}
                      className={`flex items-center justify-center w-11 sm:w-auto sm:gap-1.5 sm:px-3 h-9 sm:h-full rounded-md text-sm font-medium transition-all ${
                        viewMode === 'map'
                          ? 'bg-primary-100 text-navy-800'
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
                      className={`flex items-center justify-center w-11 sm:w-auto sm:gap-1.5 sm:px-3 h-9 sm:h-full rounded-md text-sm font-medium transition-all ${
                        viewMode === 'list'
                          ? 'bg-primary-100 text-navy-800'
                          : 'text-neutral-600 hover:text-navy-800'
                      }`}
                      aria-label="Grid view"
                      aria-pressed={viewMode === 'list'}
                    >
                      <LayoutGrid className="w-4 h-4" />
                      <span className="hidden sm:inline">Grid</span>
                    </button>
                  </div>
                </div>
              )}

              {hasSubmittedSearch && autoExpandNotice && !(loading && filteredLocations.length === 0) && (
                // amber-800 on amber-50 measures 6.84:1 (clears AA) -- a
                // noticeably different tone from the neutral results
                // header above it, read as "heads up," not an error.
                <div className="flex items-start gap-2 px-4 py-2.5 border-b border-amber-200 bg-amber-50 text-sm text-amber-800 animate-fade-in-up">
                  <HelpCircle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                  {/* The results-count text next to this already announces
                      via aria-live -- confirmed a screen reader user heard
                      the count change, but never WHY (a silent auto-widened
                      radius), or later, why zero results are showing (see
                      the matching aria-live below). */}
                  <p aria-live="polite">{autoExpandNotice}</p>
                </div>
              )}

              {/* Only shows the spinner in place of the map/grid when
                  there's genuinely nothing to show yet (the first
                  search). A refinement (Type of Care, etc.) also sets
                  loading=true while it refetches, but `locations` still
                  holds the previous results until the new ones land --
                  unmounting MapSearch in that window (the old
                  `loading ? spinner : ...` check did, unconditionally)
                  wiped its internal refs, which is what was resetting
                  the map's camera/zoom back to a fresh fit on every
                  refinement, discarding any manual pan/zoom. */}
              {loading && filteredLocations.length === 0 ? (
                <div className="h-[500px] md:h-[600px] flex items-center justify-center bg-neutral-50" role="status">
                  <div className="inline-block animate-spin rounded-full h-12 w-12 border-4 border-primary-200 border-t-primary-600" aria-hidden="true"></div>
                  <span className="sr-only">Loading search results…</span>
                </div>
              ) : viewMode === 'list' && hasSubmittedSearch ? (
                <>
                  {/* Sits directly above the provider list itself (not up
                      by the results-count/Map-Grid header) -- right before
                      someone starts tapping into individual providers,
                      which is the point it actually matters. Map view's
                      equivalent list (MapSearch.tsx's ResultsList, below
                      the map) carries the same line in the same spot
                      relative to its own list. */}
                  <p className="px-4 py-1.5 text-xs text-neutral-600 border-b border-neutral-200 bg-neutral-50">
                    AHHD doesn't endorse specific providers. Always verify accreditation and other details directly with them.
                  </p>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 p-4 min-h-[500px] bg-neutral-50 animate-fade-in-up">
                    {filteredLocations.map((loc) => (
                      <ProviderCard key={loc.location_id} location={loc} />
                    ))}
                  </div>
                </>
              ) : (
                // The animation class goes on this wrapper, not inside
                // MapSearch itself -- it's a paint-only transform/opacity
                // (doesn't affect layout), so it's safe to animate around
                // the Google Map without risking a mid-animation layout
                // measurement glitch in the Maps API.
                <div className={hasSubmittedSearch ? 'animate-fade-in-up' : ''}>
                  <MapSearch
                    locations={filteredLocations}
                    // All three withheld until a search is actually
                    // submitted -- userCoords/searchBounds/boundaryPolygon
                    // otherwise get set the moment an autocomplete
                    // suggestion is picked (before Search is even
                    // clicked), which moved the camera and drew the
                    // region outline for a location that hasn't actually
                    // been searched yet.
                    userCoords={hasSubmittedSearch ? userCoords : null}
                    searchBounds={hasSubmittedSearch ? searchBounds : null}
                    boundaryPolygon={hasSubmittedSearch ? boundaryPolygon : null}
                    boundaryLoading={hasSubmittedSearch ? boundaryLoading : false}
                    searchRegionScale={hasSubmittedSearch ? searchRegionScale : null}
                    searchGeneration={searchGeneration}
                    // The Radius circle only draws for zip/address
                    // searches. For a city, a wide circle over the whole
                    // city just competes visually with its outline and
                    // adds nothing useful; for a state, the Radius isn't
                    // even a centroid-distance concept anymore (see
                    // expandBoundsByMiles/filteredLocations above) --
                    // drawing a circle for it would actively misrepresent
                    // what's actually being searched. MapSearch's circle
                    // draws whenever radiusMiles < 999999, so passing
                    // "Any distance" here suppresses it without needing a
                    // separate prop; the real distanceRadius value is
                    // still what's shown in the Radius selector and used
                    // for actual filtering/querying above.
                    radiusMiles={searchRegionScale === 'zip' || searchRegionScale === 'address' ? distanceRadius : 999999}
                  />
                </div>
              )}
            </div>

            {hasSubmittedSearch && filteredLocations.length === 0 && !loading && (
              <div className="mt-4">
                <p className="text-neutral-600" aria-live="polite">
                  No providers found matching your criteria. Try adjusting your filters or search location.
                </p>
              </div>
            )}
          </div>
        </div>
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
