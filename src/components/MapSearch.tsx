import { useEffect, useMemo, useState, useRef } from 'react';
import { APIProvider, Map, AdvancedMarker, InfoWindow, useMap } from '@vis.gl/react-google-maps';
import type { LocationWithDetails } from '../types/database';
import { buildProviderSlug } from '../lib/slug';
import { formatDistance } from '../lib/geoUtils';
import { saveHomeScrollPosition } from '../lib/scrollRestoration';

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
  radiusMiles?: number;
  hoveredLocationId?: string | null;
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

function MapContent({ locations, userCoords, searchBounds, radiusMiles, hoveredLocationId }: MapSearchProps) {
  const map = useMap();
  const [selectedLocationId, setSelectedLocationId] = useState<string | null>(null);

  const hasZoomedToBounds = useRef<string>('');
  const hasZoomedToUser = useRef<string>('');
  const radiusCircleRef = useRef<google.maps.Circle | null>(null);
  const searchRegionRectRef = useRef<google.maps.Rectangle | null>(null);

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

  // Outlines the actual region the search is querying against -- a
  // state's real extent, or the ~50mi box expanded around a geocoded
  // city/zip/address (see SEARCH_RADIUS_DEGREES in SearchHero.tsx). A
  // plain thin outline (no fill) so it reads as a lightweight "here's the
  // searched area" reference rather than competing visually with the
  // Radius circle above, which represents a different thing (the
  // client-side distance filter, when one's actually selected). Google's
  // Rectangle doesn't support a dashed stroke directly (that needs a
  // Polyline with icon symbols) -- solid thin stroke is a reasonable
  // stand-in.
  useEffect(() => {
    if (!map) return;

    if (searchRegionRectRef.current) {
      searchRegionRectRef.current.setMap(null);
      searchRegionRectRef.current = null;
    }

    if (searchBounds) {
      searchRegionRectRef.current = new google.maps.Rectangle({
        bounds: {
          north: searchBounds.north,
          south: searchBounds.south,
          east: searchBounds.east,
          west: searchBounds.west,
        },
        strokeColor: '#6b7280',
        strokeOpacity: 0.6,
        strokeWeight: 1.5,
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
  }, [searchBounds, map]);

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
      const key = `${JSON.stringify(searchBounds)}|${locationsWithCoords.length}`;
      if (hasZoomedToBounds.current !== key) {
        hasZoomedToBounds.current = key;

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
  }, [searchBounds, userCoords, map]);

  const handlePinClick = (location: LocationWithDetails) => {
    setSelectedLocationId(location.location_id);
    if (location.latitude && location.longitude && map) {
      const newCenter = { lat: location.latitude, lng: location.longitude };
      // Google's Maps JS API doesn't expose a way to control the zoom
      // animation's speed directly -- that's internal to the renderer. The
      // two real levers are how large a zoom jump is requested (fewer new
      // tile levels to fetch/render reads as snappier) and network/tile
      // load time, which isn't controllable client-side. Pulled the target
      // back from 14 to 13 -- still close enough to read the immediate
      // area, but one fewer tile level to load on every click, especially
      // noticeable jumping from the unsearched nationwide default view.
      map.moveCamera({ center: newCenter, zoom: 13 });
    }
  };

  return (
    <>
      {locationsWithCoords.map((location) => {
        const isHovered = hoveredLocationId === location.location_id;
        return (
          <AdvancedMarker
            key={location.location_id}
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
        return (
          <InfoWindow
            position={{ lat: location.latitude!, lng: location.longitude! }}
            onCloseClick={() => setSelectedLocationId(null)}
          >
            <div className="p-2 min-w-[200px]">
              <h3 className="font-semibold text-navy-800 mb-1">
                {location.organization?.organization_name}
              </h3>
              {location.service_types && location.service_types.length > 0 && (
                <div className="flex flex-wrap gap-1.5 mb-2">
                  {location.service_types.map((st) => (
                    // navy-800 on primary-100 measures 11.33:1 (AAA) --
                    // the previously-used primary-700/primary-100 pairing
                    // elsewhere in the app only measures 4.21:1, just under
                    // even the AA floor for text this size.
                    <span
                      key={st.service_type_id}
                      className="inline-block px-2 py-0.5 bg-primary-100 text-navy-800 rounded-full text-xs font-medium"
                    >
                      {st.service_type_name}
                    </span>
                  ))}
                </div>
              )}
              <p className="text-sm text-neutral-600 mb-2">
                {location.address_line_1}<br />
                {location.city}, {location.state} {location.postal_code}
              </p>
              {(location as any).distance && (
                <p className="text-xs text-primary-600 font-medium mb-2">
                  {formatDistance((location as any).distance)} away
                </p>
              )}
              {location.public_phone && (
                <p className="text-sm text-neutral-600 mb-2">
                  {location.public_phone}
                </p>
              )}
              <button
                onClick={() => {
                  saveHomeScrollPosition();
                  window.location.href = `/provider/${buildProviderSlug(location.organization?.organization_name || location.location_name || '', location.achc_source_id)}`;
                }}
                className="mt-2 w-full text-center px-3 py-1.5 bg-primary-500 text-white rounded-md text-sm font-medium hover:bg-primary-600 transition-colors"
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

export default function MapSearch(props: MapSearchProps) {
  const { locations } = props;
  const locationsWithCoords = locations.filter(
    (loc) => loc.latitude !== null && loc.longitude !== null
  );
  // Lifted above MapContent (which renders the actual markers) since the
  // results list below is a sibling of the map, not a child of it.
  const [hoveredLocationId, setHoveredLocationId] = useState<string | null>(null);

  return (
    <div className="relative">
      <div className="h-[500px] md:h-[600px] w-full rounded-xl overflow-hidden touch-manipulation">
        <APIProvider apiKey={GOOGLE_MAPS_API_KEY}>
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
        </APIProvider>
      </div>

      {locationsWithCoords.length > 0 && (
        <div className="mt-4 bg-neutral-50 rounded-lg p-3 md:p-4 max-h-80 md:max-h-96 overflow-y-auto">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold text-navy-800 text-sm md:text-base">
              {locationsWithCoords.length} {locationsWithCoords.length === 1 ? 'Provider' : 'Providers'}
            </h3>
            <p className="text-xs text-neutral-500">Tap to view</p>
          </div>
          <div className="space-y-2">
            {locationsWithCoords.map((location) => (
              <button
                key={location.location_id}
                onMouseEnter={() => setHoveredLocationId(location.location_id)}
                onMouseLeave={() => setHoveredLocationId(null)}
                onClick={() => {
                  if (location.latitude && location.longitude) {
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
                    <p className="text-xs md:text-sm text-neutral-600">
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
                  </div>
                </div>
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
