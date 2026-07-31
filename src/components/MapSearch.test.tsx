import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import MapSearch from './MapSearch';
import type { LocationWithDetails } from '../types/database';

// @vis.gl/react-google-maps needs a real Google Maps script loaded in a
// browser; none of that is available in a test environment, so it's
// mocked out entirely. APIProvider/Map just render their children directly,
// AdvancedMarker/InfoWindow render minimal stand-ins that preserve onClick/
// children so behavior (not the real map rendering) can be tested.
const fakeMap = {
  panTo: vi.fn(),
  setZoom: vi.fn(),
  getCenter: vi.fn(),
  getBounds: vi.fn(),
};

// MarkerClusterer builds real Google Maps overlay internals on construction
// that don't exist in jsdom; the fake below only needs the two methods
// MapSearch actually calls (addMarkers/clearMarkers).
vi.mock('@googlemaps/markerclusterer', () => ({
  MarkerClusterer: class {
    addMarkers = vi.fn();
    clearMarkers = vi.fn();
  },
}));

vi.mock('@vis.gl/react-google-maps', () => ({
  APIProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  Map: ({ children }: { children: React.ReactNode }) => <div data-testid="map">{children}</div>,
  AdvancedMarker: ({
    children,
    onClick,
    position,
  }: {
    children: React.ReactNode;
    onClick?: () => void;
    position: { lat: number; lng: number };
  }) => (
    <button
      data-testid="marker"
      data-lat={position.lat}
      data-lng={position.lng}
      onClick={onClick}
    >
      {children}
    </button>
  ),
  InfoWindow: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="info-window">{children}</div>
  ),
  useMap: () => fakeMap,
}));

// google.maps.Circle is called imperatively in a useEffect (for the search
// radius overlay); stub it so that effect doesn't throw in jsdom.
beforeEach(() => {
  vi.stubGlobal('google', {
    maps: {
      Circle: vi.fn().mockImplementation(() => ({ setMap: vi.fn() })),
    },
  });
});

function makeLocation(overrides: Partial<LocationWithDetails> = {}): LocationWithDetails {
  return {
    location_id: 'loc-1',
    organization_id: 'org-1',
    address_line_1: '123 Main St',
    city: 'Springfield',
    state: 'IL',
    postal_code: '62701',
    latitude: 39.78,
    longitude: -89.65,
    achc_source_id: '12345',
    organization: { organization_id: 'org-1', organization_name: 'Test Provider' } as any,
    ...overrides,
  } as LocationWithDetails;
}

describe('MapSearch', () => {
  it('renders one marker per location that has coordinates', () => {
    const locations = [
      makeLocation({ location_id: 'a' }),
      makeLocation({ location_id: 'b', latitude: 40.1, longitude: -88.9 }),
    ];
    render(<MapSearch locations={locations} />);
    expect(screen.getAllByTestId('marker')).toHaveLength(2);
  });

  it('excludes locations with missing coordinates from the markers and the list', () => {
    const locations = [
      makeLocation({ location_id: 'has-coords' }),
      makeLocation({ location_id: 'no-coords', latitude: null, longitude: null }),
    ];
    render(<MapSearch locations={locations} />);
    expect(screen.getAllByTestId('marker')).toHaveLength(1);
    expect(screen.getByText('1 Provider')).toBeInTheDocument();
  });

  it('shows plural "Providers" once there is more than one', () => {
    const locations = [
      makeLocation({ location_id: 'a' }),
      makeLocation({ location_id: 'b', latitude: 40.1, longitude: -88.9 }),
    ];
    render(<MapSearch locations={locations} />);
    expect(screen.getByText('2 Providers')).toBeInTheDocument();
  });

  it('renders no provider-count panel at all when there are zero geocoded locations', () => {
    const locations = [makeLocation({ latitude: null, longitude: null })];
    render(<MapSearch locations={locations} />);
    expect(screen.queryByText(/Provider/)).not.toBeInTheDocument();
  });

  it('navigates to the name+achc_source_id slug (not the raw location_id) when a list item is clicked', async () => {
    const user = userEvent.setup();
    const locations = [
      makeLocation({
        location_id: 'internal-uuid-should-not-appear-in-url',
        achc_source_id: '99887',
        organization: { organization_id: 'org-1', organization_name: 'Sunrise Home Health' } as any,
      }),
    ];
    render(<MapSearch locations={locations} />);

    // jsdom throws "Not implemented: navigation" if window.location.href is
    // actually assigned; intercept the setter to capture the value instead.
    let capturedHref = '';
    Object.defineProperty(window, 'location', {
      value: {
        get href() {
          return capturedHref;
        },
        set href(value: string) {
          capturedHref = value;
        },
      },
      writable: true,
    });

    await user.click(screen.getByRole('button', { name: /Sunrise Home Health/ }));

    expect(capturedHref).toBe('/provider/sunrise-home-health-99887');
    expect(capturedHref).not.toContain('internal-uuid-should-not-appear-in-url');
  });

  it('shows the address and phone for each listed provider', () => {
    const locations = [
      makeLocation({
        address_line_1: '456 Oak Ave',
        city: 'Peoria',
        state: 'IL',
        public_phone: '555-123-4567',
      }),
    ];
    render(<MapSearch locations={locations} />);
    expect(screen.getByText(/456 Oak Ave, Peoria, IL/)).toBeInTheDocument();
    expect(screen.getByText('555-123-4567')).toBeInTheDocument();
  });
});
