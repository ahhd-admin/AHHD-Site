import { useEffect, useMemo, useState, useRef } from 'react';
import { BadgeCheck } from 'lucide-react';
import { APIProvider, Map, AdvancedMarker, InfoWindow, useMap } from '@vis.gl/react-google-maps';
import type { LocationWithDetails } from '../types/database';
import { buildProviderSlug } from '../lib/slug';
import { formatDistance } from '../lib/geoUtils';
import { saveHomeScrollPosition } from '../lib/scrollRestoration';
import { getVerifiedDate, formatVerifiedDate } from '../lib/formatVerifiedDate';
import { saveMapView, takeSavedMapView } from '../lib/mapViewRestoration';
import type { BoundaryGeoJSON } from '../lib/boundaryLookup';

interface SearchBoundsLiteral {
  south: number;
  west: number;
  north: number;
  east: number;
}

interface MapSearchProps {
  locations: LocationWithDetails[];
  userCoords?: { lat: number; lng: number } | null;
  searchBounds?: SearchBoundsLiteral | null;
  boundaryPolygon?: BoundaryGeoJSON | null;
  // True while a city/state search's real boundary trace is still in
  // flight -- see the matching comment in SearchHero.tsx. Withholds the
  // fallback bounding-box rectangle during that window instead of drawing
  // it immediately and replacing it moments later.
  boundaryLoading?: boolean;
  // Which kind of search this is -- city/state searches always attempt a
  // real traced boundary (see boundaryLoading above) and should show
  // *only* that trace or nothing at all, never the bounding-box rectangle
  // fallback: the rectangle is a plain lat/lng rectangle, not the region's
  // real shape, and for an odd-shaped state (Maine) or a city hugging
  // another state's border, that rectangle visibly includes neighboring
  // territory that has nothing to do with the search -- confirmed live
  // (Quebec/New Hampshire inside Maine's own bounding rectangle). ZIP/
  // address searches never attempt a trace at all (see shouldTraceBoundary
  // in SearchHero.tsx), so the rectangle is their only, and reasonably
  // accurate, region indicator -- unaffected by this.
  searchRegionScale?: 'state' | 'city' | 'zip' | 'address' | null;
  radiusMiles?: number;
  hoveredLocationId?: string | null;
  // Bumped only when a search is actually (re-)submitted (Search/Enter,
  // a suggestion, "My Location") -- NOT when Type of Care or other
  // filters change the result set for the same search. Drives whether
  // the map re-fits its camera; see the fitBounds effect below for why
  // that distinction matters.
  searchGeneration?: number;
}

const GOOGLE_MAPS_API_KEY = import.meta.env.VITE_GOOGLE_MAPS_API_KEY || '';

const DEFAULT_CENTER = { lat: 39.8283, lng: -95.5795 };
const DEFAULT_ZOOM = 4.2;

// "ahhd-provider-map" was a placeholder name, not a real Map ID -- Advanced
// Markers require an actual Map ID registered in Google Cloud Console with
// vector rendering enabled, or they fail to render (the base map still
// loads fine either way, which is why this can look like "the map works but
// pins don't"). Google's DEMO_MAP_ID works immediately with no Cloud
// Console setup, for exactly this situation -- it's fine for now but shows
// a small "for development purposes only" watermark and should be swapped
// for a real registered Map ID before production launch.
const MAP_ID = 'DEMO_MAP_ID';

function MapContent({ locations, userCoords, searchBounds, boundaryPolygon, boundaryLoading, searchRegionScale, radiusMiles, hoveredLocationId, searchGeneration }: MapSearchProps) {
  const map = useMap();
  const [selectedLocationId, setSelectedLocationId] = useState<string | null>(null);

  // Google's own InfoWindow close button (.gm-ui-hover-effect) can't be
  // hidden with a normal CSS rule -- confirmed live that even an
  // external stylesheet rule with !important doesn't win against it (its
  // display is set via a direct inline style, and something about how
  // Google's renderer manages that element keeps it showing regardless).
  // Setting the property via the JS CSSOM API with 'important' priority,
  // directly on the actual element, is what actually sticks. A
  // MutationObserver re-applies it if Google re-renders/re-adds the
  // button (e.g. switching between pins), since a one-time hide on open
  // wouldn't survive that. Runs whenever a pin is selected -- our own
  // close button (see the InfoWindow content below) replaces this one.
  useEffect(() => {
    if (!selectedLocationId) return;
    const hideNativeCloseButton = () => {
      document.querySelectorAll('.gm-style-iw-chr .gm-ui-hover-effect').forEach((el) => {
        (el as HTMLElement).style.setProperty('display', 'none', 'important');
      });
    };
    hideNativeCloseButton();
    const observer = new MutationObserver(hideNativeCloseButton);
    observer.observe(document.body, { childList: true, subtree: true });
    return () => observer.disconnect();
  }, [selectedLocationId]);

  // Keyed by location_id so the InfoWindow (see below) can anchor itself to
  // the actual marker DOM element rather than a raw lat/lng -- letting
  // Google compute the tail's pixel offset from the marker's real rendered
  // size/anchor instead of guessing a fixed offset that would only be
  // correct for one hover state.
  const markerRefsMap = useRef<globalThis.Map<string, google.maps.marker.AdvancedMarkerElement>>(new globalThis.Map());
  const hasZoomedToUser = useRef<string>('');
  const radiusCircleRef = useRef<google.maps.Circle | null>(null);
  const searchRegionRectRef = useRef<google.maps.Rectangle | null>(null);
  const boundaryDataRef = useRef<google.maps.Data | null>(null);
  // Which searchGeneration the camera last fit to, and whether that fit
  // already used real markers (as opposed to just the searched region's
  // bounds, before results arrived) -- see the fitBounds effect below.
  const lastFitGenerationRef = useRef<number>(-1);
  const hasFitWithMarkersRef = useRef<boolean>(false);

  // Memoized so this is a stable reference across renders that don't
  // actually change the location data -- without this, the marker-sync
  // effect below (keyed on this array) fired on every render, not just
  // when locations genuinely changed, repeatedly tearing down and
  // rebuilding the clusterer's marker set and fighting with its own
  // zoom/pan-driven re-clustering.
  const locationsWithCoords = useMemo(
    () => locations.filter((loc) => loc.latitude !== null && loc.longitude !== null),
    [locations]
  );

  useEffect(() => {
    if (!map) return;

    if (userCoords && radiusMiles && radiusMiles < 999999) {
      if (radiusCircleRef.current) {
        radiusCircleRef.current.setMap(null);
      }

      radiusCircleRef.current = new google.maps.Circle({
        center: userCoords,
        radius: radiusMiles * 1609.34,
        strokeColor: '#2563eb',
        strokeOpacity: 0.6,
        strokeWeight: 2,
        fillColor: '#3b82f6',
        fillOpacity: 0.15,
        map: map,
      });
    } else if (radiusCircleRef.current) {
      radiusCircleRef.current.setMap(null);
      radiusCircleRef.current = null;
    }

    return () => {
      if (radiusCircleRef.current) {
        radiusCircleRef.current.setMap(null);
      }
    };
  }, [userCoords, radiusMiles, map]);

  // Outlines the actual region the search covers. Prefers the real traced
  // shape (a city's/state's actual outline, via the boundary-lookup Edge
  // Function -- see the effect below) when one was found; falls back to a
  // plain rectangle (the ~50mi query box, or a state's real extent when
  // that IS the real shape) when it wasn't -- most ZIP codes aren't
  // mapped as polygons in OpenStreetMap, or the lookup can fail for any
  // reason. Never both at once. searchBounds/boundaryPolygon only ever
  // arrive here once a search is actually submitted (SearchHero
  // withholds them before that), so this never shows for a location
  // that's merely been picked from the autocomplete dropdown but not
  // searched yet. Bold violet -- distinct from the red pins and the blue
  // Radius circle, and visible against a dense cluster of dots.
  //
  // Withheld entirely while boundaryLoading is true (a city/state trace is
  // still in flight) -- drawing the rectangle immediately and then tearing
  // it down the moment the real outline (or "no boundary found") arrives a
  // few hundred ms later read as a big rectangle flashing over the map.
  // zip/address searches never set boundaryLoading (they never attempt a
  // trace), so their rectangle still appears immediately, unaffected.
  useEffect(() => {
    if (!map) return;

    if (searchRegionRectRef.current) {
      searchRegionRectRef.current.setMap(null);
      searchRegionRectRef.current = null;
    }

    const canShowRectangle = searchRegionScale !== 'city' && searchRegionScale !== 'state';
    if (searchBounds && !boundaryPolygon && !boundaryLoading && canShowRectangle) {
      searchRegionRectRef.current = new google.maps.Rectangle({
        bounds: {
          north: searchBounds.north,
          south: searchBounds.south,
          east: searchBounds.east,
          west: searchBounds.west,
        },
        strokeColor: '#7c3aed',
        strokeOpacity: 0.9,
        strokeWeight: 3,
        fillOpacity: 0,
        clickable: false,
        map,
      });
    }

    return () => {
      if (searchRegionRectRef.current) {
        searchRegionRectRef.current.setMap(null);
      }
    };
  }, [searchBounds, boundaryPolygon, boundaryLoading, searchRegionScale, map]);

  // The real traced boundary, when the lookup found one. google.maps.Data
  // accepts GeoJSON natively and handles Polygon/MultiPolygon without
  // needing to manually walk the ring structure the way a Polygon overlay
  // would require.
  useEffect(() => {
    if (!map) return;

    if (boundaryDataRef.current) {
      boundaryDataRef.current.setMap(null);
      boundaryDataRef.current = null;
    }

    if (boundaryPolygon) {
      const data = new google.maps.Data();
      data.addGeoJson({ type: 'Feature', properties: {}, geometry: boundaryPolygon });
      data.setStyle({
        strokeColor: '#7c3aed',
        strokeOpacity: 0.9,
        strokeWeight: 3,
        fillOpacity: 0,
        clickable: false,
      });
      data.setMap(map);
      boundaryDataRef.current = data;
    }

    return () => {
      if (boundaryDataRef.current) {
        boundaryDataRef.current.setMap(null);
      }
    };
  }, [boundaryPolygon, map]);

  useEffect(() => {
    if (!map) return;

    // searchBounds wins whenever it's present -- it's the real geographic
    // extent of whatever was searched (a whole state, a city, a zip).
    // Geocoding happens once, in SearchHero (both to set this and to sort
    // results by distance) -- this used to re-geocode a second time here
    // independently, which not only duplicated the API call but also lost
    // the race against the sibling userCoords effect below (userCoords is
    // set by that same search and always got applied first with a fixed
    // zoom:11, which is why a state search kept zooming in instead of
    // fitting the state).
    if (searchBounds) {
      const generation = searchGeneration ?? 0;
      if (lastFitGenerationRef.current !== generation) {
        // A genuinely new search (Search/Enter, a suggestion, "My
        // Location") -- allow fitting again, and reset the "already did
        // our final fit" lock below.
        lastFitGenerationRef.current = generation;
        hasFitWithMarkersRef.current = false;
      }

      // Refining Type of Care (or anything else that changes the result
      // count for the SAME search) should only change which pins show,
      // not move the camera again -- once this generation has fit to real
      // markers, further changes to locationsWithCoords for that same
      // generation don't trigger another fit. Until real markers exist
      // yet (the fetch is still in flight when this first runs), this
      // stays unlocked so the effect can re-fit tightly once they arrive.
      if (!hasFitWithMarkersRef.current) {
        // Google's official state/region boundary is often much bigger
        // than where any actual result is -- Hawaii's includes the
        // uninhabited Northwestern Hawaiian Islands out past -178, which
        // pulls the boundary's center into open ocean, nowhere near the
        // populated islands the results are actually on. Fitting to the
        // real result coordinates instead centers on where the data
        // actually is, and incidentally crops in tighter for states like
        // Alaska/Illinois whose bounding rectangle has a lot of empty
        // corner area no result ever falls inside. searchBounds is only
        // the fallback now, for a region with zero matching results (or
        // exactly one, where a bare point isn't a useful "fit").
        const bounds = new google.maps.LatLngBounds();
        if (locationsWithCoords.length > 1) {
          locationsWithCoords.forEach((loc) => {
            bounds.extend({ lat: loc.latitude!, lng: loc.longitude! });
          });
        } else {
          bounds.extend({ lat: searchBounds.south, lng: searchBounds.west });
          bounds.extend({ lat: searchBounds.north, lng: searchBounds.east });
          if (locationsWithCoords.length === 1) {
            bounds.extend({
              lat: locationsWithCoords[0].latitude!,
              lng: locationsWithCoords[0].longitude!,
            });
          }
        }

        if (!bounds.isEmpty()) {
          // Marker-only bounds hug whatever's outermost, which for a
          // state where results don't happen to reach every edge (e.g.
          // no provider right at Illinois's northern or southern tip)
          // reads as the state itself being cropped, not just a tight
          // fit. A proportional buffer (not a fixed political boundary --
          // that's what caused the Hawaii-in-the-ocean bug) gives some
          // breathing room past the outermost real markers, sized to the
          // cluster's own extent so it scales sensibly for both a tiny
          // single-city search and a whole-state one. The floor (~3-4
          // miles) keeps a small local cluster from staying zoomed to a
          // single block.
          const ne = bounds.getNorthEast();
          const sw = bounds.getSouthWest();
          const latBuffer = Math.max((ne.lat() - sw.lat()) * 0.12, 0.05);
          const lngBuffer = Math.max((ne.lng() - sw.lng()) * 0.12, 0.05);
          bounds.extend({ lat: ne.lat() + latBuffer, lng: ne.lng() + lngBuffer });
          bounds.extend({ lat: sw.lat() - latBuffer, lng: sw.lng() - lngBuffer });

          map.fitBounds(bounds, { top: 40, right: 56, bottom: 40, left: 56 });

          if (locationsWithCoords.length > 0) {
            // Fit real markers, not just the searched region's own
            // bounds -- lock further fits for this generation so a later
            // Type of Care refinement (changing which/how many pins show
            // for this SAME search) doesn't move the camera again.
            hasFitWithMarkersRef.current = true;
          }

          // Dev-only: fitBounds settles asynchronously, so the camera's
          // final center/zoom aren't known until the map reports 'idle'.
          // Logging them lets a specific problem case (e.g. "Alaska looks
          // off-center") be reported back with exact numbers instead of a
          // description -- open the browser console, run the search, and
          // copy the logged object.
          if (import.meta.env.DEV) {
            google.maps.event.addListenerOnce(map, 'idle', () => {
              const center = map.getCenter();
              const finalZoom = map.getZoom();
              console.log('[map fit]', {
                resultCount: locationsWithCoords.length,
                geocodedRegion: searchBounds,
                fitBoundsTarget: {
                  south: bounds.getSouthWest().lat(),
                  west: bounds.getSouthWest().lng(),
                  north: bounds.getNorthEast().lat(),
                  east: bounds.getNorthEast().lng(),
                },
                settledCenter: center ? { lat: center.lat(), lng: center.lng() } : null,
                settledZoom: finalZoom,
              });
            });
          }
        }
      }
      return;
    }

    // No bounds means this is a real point location -- either the "My
    // Location" GPS button, or a search result that only ever geocodes to
    // a point. Either way "zoom in close" is the right behavior.
    if (userCoords) {
      const coordKey = `${userCoords.lat},${userCoords.lng}`;
      if (hasZoomedToUser.current !== coordKey) {
        hasZoomedToUser.current = coordKey;
        // A single camera move instead of an animated panTo followed by a
        // separate setZoom -- the two-step version watches an animated
        // pan finish before the zoom (and its tile fetch) even starts,
        // which reads as slower than the actual tile-loading time alone.
        map.moveCamera({ center: userCoords, zoom: 11 });
      }
    }
  }, [searchBounds, userCoords, map, searchGeneration, locationsWithCoords]);

  // Restores whichever pin's InfoWindow was open, and the exact camera
  // position, from just before a click-through to a provider detail page
  // -- see mapViewRestoration.ts for why this exists. Runs once real
  // markers exist (so the "this pin still exists in the current result
  // set" check below is meaningful) and, since it runs after the fitBounds
  // effect above in the same commit, its camera move wins over that
  // effect's own initial fit -- consuming the saved view immediately
  // either way so it doesn't linger and reapply itself on some unrelated
  // later render.
  const hasRestoredViewRef = useRef(false);
  useEffect(() => {
    if (!map || hasRestoredViewRef.current || locationsWithCoords.length === 0) return;
    hasRestoredViewRef.current = true;

    const saved = takeSavedMapView();
    if (!saved) return;

    if (saved.center && saved.zoom !== null) {
      map.moveCamera({ center: saved.center, zoom: saved.zoom });
    }
    if (saved.selectedLocationId && locationsWithCoords.some((l) => l.location_id === saved.selectedLocationId)) {
      setSelectedLocationId(saved.selectedLocationId);
    }
    // Locks out the fitBounds effect above for this generation so it
    // doesn't override the just-restored camera on a later render (e.g.
    // once services/data finish settling).
    lastFitGenerationRef.current = searchGeneration ?? 0;
    hasFitWithMarkersRef.current = true;
  }, [map, locationsWithCoords, searchGeneration]);

  const handlePinClick = (location: LocationWithDetails) => {
    setSelectedLocationId(location.location_id);
    if (location.latitude && location.longitude && map) {
      const newCenter = { lat: location.latitude, lng: location.longitude };
      // Google's Maps JS API doesn't expose a way to control the zoom
      // animation's speed directly -- that's internal to the renderer. The
      // two real levers are how large a zoom jump is requested (fewer new
      // tile levels to fetch/render reads as snappier) and network/tile
      // load time, which isn't controllable client-side.
      //
      // Confirmed via a Playwright video/screenshot-burst capture (still
      // reported as "the map blows up/zooms in weird" after the earlier
      // tap-highlight fix -- a genuinely different bug from that one) that
      // jumping straight to a fixed zoom=13 from a wide result-set fit
      // (e.g. a whole-metro view around zoom 9-10 after a city search)
      // shows a clearly visible blown-up, pixelated tile for about one
      // frame: Google's renderer shows an upscaled version of the
      // already-loaded low-zoom tile while the real zoom=13 tiles are
      // still being fetched, before snapping to the sharp result a beat
      // later. The fix isn't fetch speed (not controllable client-side);
      // it's requesting a smaller jump. A +4 cap still landed on the same
      // zoom=13 as before in the common case (fitBounds typically settles
      // around zoom 9-10, and 9+4/10+4 both round up to 13 anyway) and
      // produced an identical blur in testing -- +2 was the smallest cap
      // that visibly cleared the artifact while still zooming in
      // meaningfully. Clicking again from the new (now closer) position
      // closes the rest of the way with another small, clean jump instead
      // of one big jarring one.
      const currentZoom = map.getZoom() ?? 4;
      const targetZoom = Math.min(13, currentZoom + 2);
      map.moveCamera({ center: newCenter, zoom: targetZoom });
    }
  };

  return (
    <>
      {locationsWithCoords.map((location) => {
        const isHovered = hoveredLocationId === location.location_id;
        return (
          <AdvancedMarker
            key={location.location_id}
            ref={(marker) => {
              if (marker) markerRefsMap.current.set(location.location_id, marker);
              else markerRefsMap.current.delete(location.location_id);
            }}
            position={{ lat: location.latitude!, lng: location.longitude! }}
            onClick={() => handlePinClick(location)}
            zIndex={isHovered ? 1 : 0}
            // AdvancedMarker wraps children in its own content element,
            // separate from the inner <div> below -- styling only the
            // inner div (the previous fix) missed this outer element's
            // own default tap/focus highlight, which is the more likely
            // source of the still-reported pixelated flash on click: it
            // scales up along with the map tiles during the zoom.
            className="outline-none [-webkit-tap-highlight-color:transparent] focus:outline-none focus-visible:outline-none"
          >
            <div
              style={{
                backgroundColor: '#ef4444',
                border: '3px solid white',
                borderRadius: '50%',
                width: isHovered ? '32px' : '24px',
                height: isHovered ? '32px' : '24px',
                cursor: 'pointer',
                // A visible ring, not just a size bump -- distinct from the
                // "selected" state (opens the InfoWindow), which doesn't
                // change the marker's own appearance at all.
                boxShadow: isHovered ? '0 0 0 4px rgba(239,68,68,0.35)' : 'none',
                transition: 'width 0.15s ease, height 0.15s ease, box-shadow 0.15s ease',
                // The browser's default tap/focus highlight renders as
                // a solid rectangle sized to the marker's hit area (not
                // the visible 24px circle), and during the click-driven
                // zoom transition that highlight gets scaled up with
                // the map tiles -- which reads as a big pixelated flash
                // right as you click, before the zoom settles.
                WebkitTapHighlightColor: 'transparent',
                outline: 'none',
              }}
            />
          </AdvancedMarker>
        );
      })}

      {selectedLocationId && locationsWithCoords.find(l => l.location_id === selectedLocationId) && (() => {
        const location = locationsWithCoords.find(l => l.location_id === selectedLocationId)!;
        const verifiedLabel = formatVerifiedDate(getVerifiedDate(location));
        return (
          <InfoWindow
            // Anchored to the marker's own DOM element rather than a raw
            // lat/lng -- Google then computes the tail's pixel offset from
            // the marker's actual rendered anchor point (bottom-center of
            // the pin circle) instead of opening at the exact coordinate,
            // which previously put the tail overlapping the pin rather
            // than pointing cleanly at its top. Falls back to position for
            // the one render where a marker's ref hasn't attached yet.
            anchor={markerRefsMap.current.get(location.location_id) ?? undefined}
            position={
              markerRefsMap.current.has(location.location_id)
                ? undefined
                : { lat: location.latitude!, lng: location.longitude! }
            }
            onCloseClick={() => setSelectedLocationId(null)}
          >
            {/* Condensed: address/city/state/zip collapsed onto one line,
                distance and phone sharing a row instead of a paragraph
                each -- shorter card, same information. */}
            <div className="p-1.5 min-w-[190px] max-w-[240px]">
              {/* Google's own InfoWindow close button is hidden globally
                  (see index.css) -- it reserves a fixed area at the
                  top-right regardless of our content, floating well above
                  the title with no way to align it. This custom button
                  sits in the same flex row as the name instead, top-
                  aligned with its first line (items-start) rather than
                  vertically centered against the full (possibly
                  two-line) height, and never overlaps the text since it
                  has its own flex-shrink-0 column and the title has
                  right-padding reserved for it. */}
              <div className="flex items-start justify-between gap-1.5 mb-1">
                <h3 className="font-semibold text-navy-800 text-sm leading-snug">
                  {location.organization?.organization_name}
                </h3>
                <button
                  type="button"
                  onClick={() => setSelectedLocationId(null)}
                  aria-label="Close"
                  className="flex-shrink-0 text-neutral-400 hover:text-neutral-700 focus:outline-none focus:ring-2 focus:ring-primary-200 rounded"
                >
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-4 h-4">
                    <path d="M6.28 5.22a.75.75 0 0 0-1.06 1.06L8.94 10l-3.72 3.72a.75.75 0 1 0 1.06 1.06L10 11.06l3.72 3.72a.75.75 0 1 0 1.06-1.06L11.06 10l3.72-3.72a.75.75 0 0 0-1.06-1.06L10 8.94 6.28 5.22Z" />
                  </svg>
                </button>
              </div>
              {location.service_types && location.service_types.length > 0 && (
                <div className="flex flex-wrap gap-1 mb-1.5">
                  {location.service_types.map((st) => (
                    // navy-800 on primary-100 measures 11.33:1 (AAA) --
                    // the previously-used primary-700/primary-100 pairing
                    // elsewhere in the app only measures 4.21:1, just under
                    // even the AA floor for text this size.
                    <span
                      key={st.service_type_id}
                      className="inline-block px-1.5 py-0.5 bg-primary-100 text-navy-800 rounded-full text-[11px] font-medium leading-none"
                    >
                      {st.service_type_name}
                    </span>
                  ))}
                </div>
              )}
              <p className="text-xs text-neutral-600 mb-1">
                {location.address_line_1}, {location.city}, {location.state} {location.postal_code}
              </p>
              {((location as any).distance || location.public_phone) && (
                <p className="text-xs text-neutral-600 mb-1">
                  {(location as any).distance && (
                    <span className="text-primary-600 font-medium">
                      {formatDistance((location as any).distance)} away
                    </span>
                  )}
                  {(location as any).distance && location.public_phone && ' • '}
                  {location.public_phone}
                </p>
              )}
              {verifiedLabel && (
                <p className="flex items-center gap-1 text-[11px] font-medium text-success-700 mb-1.5">
                  <BadgeCheck className="w-3 h-3 flex-shrink-0" />
                  {verifiedLabel}
                </p>
              )}
              <button
                onClick={() => {
                  const center = map?.getCenter();
                  saveMapView({
                    selectedLocationId: location.location_id,
                    center: center ? { lat: center.lat(), lng: center.lng() } : null,
                    zoom: map?.getZoom() ?? null,
                  });
                  saveHomeScrollPosition();
                  window.location.href = `/provider/${buildProviderSlug(location.organization?.organization_name || location.location_name || '', location.achc_source_id)}`;
                }}
                className="mt-1 w-full text-center px-3 py-1.5 bg-primary-500 text-white rounded-md text-sm font-medium hover:bg-primary-600 transition-colors"
              >
                View Details
              </button>
            </div>
          </InfoWindow>
        );
      })()}
    </>
  );
}

interface ResultsListProps {
  locations: LocationWithDetails[];
  hoveredLocationId: string | null;
  setHoveredLocationId: (id: string | null) => void;
}

// A sibling of <Map>, not a child of it -- but still inside <APIProvider>,
// which is what makes useMap() resolve here (the default map instance is
// registered on APIProviderContext, reachable by any of its descendants,
// not just descendants of the <Map> that created it). That access is what
// lets a click on a list item capture the map's current camera before
// navigating away, the same as a pin click's "View Details" button does.
function ResultsList({ locations, hoveredLocationId, setHoveredLocationId }: ResultsListProps) {
  const map = useMap();

  if (locations.length === 0) return null;

  return (
    <div className="mt-4 bg-neutral-50 rounded-lg p-3 md:p-4 max-h-80 md:max-h-96 overflow-y-auto">
      <div className="flex items-center justify-between mb-3">
        <h3 className="font-semibold text-navy-800 text-sm md:text-base">
          {locations.length} {locations.length === 1 ? 'Provider' : 'Providers'}
        </h3>
        <p className="text-xs text-neutral-500">Tap to view</p>
      </div>
      <div className="space-y-2">
        {locations.map((location) => (
          <button
            key={location.location_id}
            onMouseEnter={() => setHoveredLocationId(location.location_id)}
            onMouseLeave={() => setHoveredLocationId(null)}
            onClick={() => {
              if (location.latitude && location.longitude) {
                const center = map?.getCenter();
                saveMapView({
                  selectedLocationId: location.location_id,
                  center: center ? { lat: center.lat(), lng: center.lng() } : null,
                  zoom: map?.getZoom() ?? null,
                });
                saveHomeScrollPosition();
                window.location.href = `/provider/${buildProviderSlug(location.organization?.organization_name || location.location_name || '', location.achc_source_id)}`;
              }
            }}
            className={`w-full text-left p-2.5 md:p-3 bg-white rounded-lg border transition-all group active:scale-98 ${
              hoveredLocationId === location.location_id
                ? 'border-primary-400 shadow-md'
                : 'border-neutral-200'
            } hover:border-primary-400 hover:shadow-md active:border-primary-500`}
          >
            <div className="flex items-start gap-2 md:gap-3">
              <div className="flex-1 min-w-0">
                <h4 className="font-semibold truncate transition-colors text-sm md:text-base text-navy-800 group-hover:text-primary-600">
                  {location.organization?.organization_name}
                </h4>
                {location.service_types && location.service_types.length > 0 && (
                  <div className="flex flex-wrap gap-1 mt-1">
                    {location.service_types.map((st) => (
                      <span
                        key={st.service_type_id}
                        className="inline-block px-1.5 py-0.5 bg-primary-100 text-navy-800 rounded-full text-[11px] font-medium leading-none"
                      >
                        {st.service_type_name}
                      </span>
                    ))}
                  </div>
                )}
                <p className="text-xs md:text-sm text-neutral-600 mt-1">
                  {location.address_line_1}, {location.city}, {location.state}
                </p>
                {(location as any).distance && (
                  <p className="text-xs text-primary-600 font-medium mt-1">
                    {formatDistance((location as any).distance)} away
                  </p>
                )}
                {location.public_phone && (
                  <p className="text-xs text-neutral-500 mt-1">
                    {location.public_phone}
                  </p>
                )}
                {(() => {
                  const verifiedLabel = formatVerifiedDate(getVerifiedDate(location));
                  return verifiedLabel ? (
                    <p className="flex items-center gap-1 text-[11px] font-medium text-success-700 mt-1">
                      <BadgeCheck className="w-3 h-3 flex-shrink-0" />
                      {verifiedLabel}
                    </p>
                  ) : null;
                })()}
              </div>
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}

export default function MapSearch(props: MapSearchProps) {
  const { locations } = props;
  const locationsWithCoords = locations.filter(
    (loc) => loc.latitude !== null && loc.longitude !== null
  );
  // Lifted above MapContent (which renders the actual markers) since the
  // results list below is a sibling of the map, not a child of it.
  const [hoveredLocationId, setHoveredLocationId] = useState<string | null>(null);

  return (
    // Wraps both the map and the results list below it (not just the map)
    // so ResultsList can reach the map instance via useMap() -- see the
    // comment on that component for why that requires being inside
    // APIProvider, not just visually below the map.
    <APIProvider apiKey={GOOGLE_MAPS_API_KEY}>
      <div className="relative">
        <div className="h-[500px] md:h-[600px] w-full rounded-xl overflow-hidden touch-manipulation">
          <Map
            defaultCenter={DEFAULT_CENTER}
            defaultZoom={DEFAULT_ZOOM}
            mapId={MAP_ID}
            gestureHandling="greedy"
            disableDefaultUI={false}
            scrollwheel={true}
            clickableIcons={false}
            mapTypeControl={false}
            fullscreenControl={true}
            streetViewControl={false}
            zoomControl={true}
            // No pan restriction -- a bounded box kept causing exactly the
            // Alaska/Hawaii positioning problems it was meant to prevent
            // (Google silently recenters the camera to whatever fits
            // inside the restriction when a fitBounds target doesn't,
            // which is what made those searches look off-center/missing).
            // The initial view is still centered on the contiguous US via
            // DEFAULT_CENTER/DEFAULT_ZOOM below regardless of this.
            minZoom={3}
          >
            <MapContent {...props} hoveredLocationId={hoveredLocationId} />
          </Map>
        </div>

        <ResultsList
          locations={locationsWithCoords}
          hoveredLocationId={hoveredLocationId}
          setHoveredLocationId={setHoveredLocationId}
        />
      </div>
    </APIProvider>
  );
}
