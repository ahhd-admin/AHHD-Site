import { useEffect, useMemo, useState, useRef } from 'react';
import { APIProvider, Map, AdvancedMarker, InfoWindow, useMap } from '@vis.gl/react-google-maps';
import type { LocationWithDetails } from '../types/database';
import { buildProviderSlug } from '../lib/slug';
import { formatDistance } from '../lib/geoUtils';

interface MapSearchProps {
  locations: LocationWithDetails[];
  searchLocation?: string;
  userCoords?: { lat: number; lng: number } | null;
  radiusMiles?: number;
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

function MapContent({ locations, searchLocation, userCoords, radiusMiles }: MapSearchProps) {
  const map = useMap();
  const [selectedLocationId, setSelectedLocationId] = useState<string | null>(null);

  const hasZoomedToSearch = useRef<string>('');
  const hasZoomedToUser = useRef<string>('');
  const radiusCircleRef = useRef<google.maps.Circle | null>(null);

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

  useEffect(() => {
    if (userCoords && map) {
      const coordKey = `${userCoords.lat},${userCoords.lng}`;
      if (hasZoomedToUser.current !== coordKey) {
        hasZoomedToUser.current = coordKey;
        hasZoomedToSearch.current = '';

        const newZoom = 11;
        // A single camera move instead of an animated panTo followed by a
        // separate setZoom -- the two-step version watches an animated
        // pan finish before the zoom (and its tile fetch) even starts,
        // which reads as slower than the actual tile-loading time alone.
        map.moveCamera({ center: userCoords, zoom: newZoom });
      }
    }
  }, [userCoords, map]);

  useEffect(() => {
    const geocodeLocation = async (locationQuery: string) => {
      if (!map || !locationQuery.trim()) return;

      if (hasZoomedToSearch.current === locationQuery) return;
      hasZoomedToSearch.current = locationQuery;

      try {
        const response = await fetch(
          `https://maps.googleapis.com/maps/api/geocode/json?address=${encodeURIComponent(locationQuery)}&key=${GOOGLE_MAPS_API_KEY}`
        );
        const data = await response.json();

        if (data.status === 'OK' && data.results && data.results.length > 0) {
          const result = data.results[0];

          // `bounds` is the real administrative extent of the result (only
          // present for regions -- states, cities, zips); a street-address
          // result only gets `viewport`, a smaller "reasonable to display"
          // box, and a bare point has neither. fitBounds computes whatever
          // center/zoom actually fits whichever box is available itself --
          // unlike the old manual zoom-from-viewport-width formula, it
          // doesn't need per-region-size tuning, so ONE fixed pixel padding
          // correctly fits every state from Rhode Island to Alaska without
          // a lookup table. 48px reads as a comfortable margin at both the
          // 500px (mobile) and 600px (desktop) map heights used below.
          const box = result.geometry.bounds || result.geometry.viewport;

          if (box) {
            const bounds = new google.maps.LatLngBounds(
              { lat: box.southwest.lat, lng: box.southwest.lng },
              { lat: box.northeast.lat, lng: box.northeast.lng }
            );
            map.fitBounds(bounds, 48);
          } else {
            map.moveCamera({
              center: { lat: result.geometry.location.lat, lng: result.geometry.location.lng },
              zoom: 12,
            });
          }
        }
      } catch (error) {
        console.error('Geocoding error:', error);
      }
    };

    if (searchLocation && searchLocation.trim() && !userCoords) {
      geocodeLocation(searchLocation);
    }
  }, [searchLocation, userCoords, map]);

  const handlePinClick = (location: LocationWithDetails) => {
    setSelectedLocationId(location.location_id);
    if (location.latitude && location.longitude && map) {
      const newCenter = { lat: location.latitude, lng: location.longitude };
      // Jumping the zoom level here is often a big jump (e.g. from a
      // nationwide zoom straight to 14), which needs many new tile levels
      // either way -- moveCamera at least avoids adding a separate
      // animated pan on top of that tile-loading wait.
      map.moveCamera({ center: newCenter, zoom: 14 });
    }
  };

  return (
    <>
      {locationsWithCoords.map((location) => (
              <AdvancedMarker
                key={location.location_id}
                position={{ lat: location.latitude!, lng: location.longitude! }}
                onClick={() => handlePinClick(location)}
              >
                <div
                  style={{
                    backgroundColor: '#ef4444',
                    border: '3px solid white',
                    borderRadius: '50%',
                    width: '24px',
                    height: '24px',
                    cursor: 'pointer',
                  }}
                />
              </AdvancedMarker>
            ))}

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
            // Wide enough to include Alaska and Hawaii (both have real
            // providers in the data) in addition to the contiguous states --
            // the old bounds (24.5-49.5N, -125 to -66) were continental-US
            // only and actively blocked panning to either. The default
            // view still opens centered on the contiguous states (see
            // DEFAULT_CENTER/DEFAULT_ZOOM below); this only affects how far
            // someone can pan/zoom to reach AK/HI when they need to.
            restriction={{
              latLngBounds: {
                north: 72,
                south: 18,
                west: -170.0,
                east: -66.0
              },
              strictBounds: false
            }}
            minZoom={3}
          >
            <MapContent {...props} />
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
                onClick={() => {
                  if (location.latitude && location.longitude) {
                    window.location.href = `/provider/${buildProviderSlug(location.organization?.organization_name || location.location_name || '', location.achc_source_id)}`;
                  }
                }}
                className="w-full text-left p-2.5 md:p-3 bg-white rounded-lg border transition-all group active:scale-98 border-neutral-200 hover:border-primary-400 hover:shadow-md active:border-primary-500"
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
