import { useEffect, useState, useRef } from 'react';
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

function MapContent({ locations, searchLocation, userCoords, radiusMiles }: MapSearchProps) {
  const map = useMap();
  const [selectedLocationId, setSelectedLocationId] = useState<string | null>(null);

  const hasZoomedToSearch = useRef<string>('');
  const hasZoomedToUser = useRef<string>('');
  const radiusCircleRef = useRef<google.maps.Circle | null>(null);

  const locationsWithCoords = locations.filter(
    (loc) => loc.latitude !== null && loc.longitude !== null
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
        map.panTo(userCoords);
        map.setZoom(newZoom);
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
          const newCenter = {
            lat: result.geometry.location.lat,
            lng: result.geometry.location.lng
          };

          const viewport = result.geometry.viewport;
          let zoomLevel = 12;

          if (viewport) {
            const GLOBE_WIDTH = 256;
            const west = viewport.southwest.lng;
            const east = viewport.northeast.lng;
            const angle = east - west;
            if (angle > 0) {
              const zoom = Math.round(Math.log(window.innerWidth * 360 / angle / GLOBE_WIDTH) / Math.LN2);
              zoomLevel = Math.max(8, Math.min(zoom - 1, 15));
            }
          }

          map.panTo(newCenter);
          map.setZoom(zoomLevel);
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
      map.panTo(newCenter);
      map.setZoom(14);
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
                    boxShadow: '0 2px 8px rgba(0,0,0,0.3)',
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
            mapId="ahhd-provider-map"
            gestureHandling="greedy"
            disableDefaultUI={false}
            scrollwheel={true}
            clickableIcons={false}
            mapTypeControl={false}
            fullscreenControl={true}
            streetViewControl={false}
            zoomControl={true}
            restriction={{
              latLngBounds: {
                north: 49.5,
                south: 24.5,
                west: -125.0,
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
