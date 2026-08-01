// Provider links are plain <a href> navigations -- a real page load, not
// client-side routing -- so hitting Back reloads the home page from
// scratch: the map remounts and refits its camera to the whole result set,
// and whichever pin's InfoWindow was open is gone. That's a jarring loss
// of context for someone who zoomed/panned to a specific pin, opened it,
// and clicked through -- Back should put them right back where they were,
// not make them re-find their spot. Saved right before navigating to a
// provider page (see the call sites in MapSearch.tsx) and consumed once on
// the next mount (see the restore effect in MapContent).
export interface SavedMapView {
  selectedLocationId: string | null;
  center: { lat: number; lng: number } | null;
  zoom: number | null;
}

const MAP_VIEW_STORAGE_KEY = 'ahhd:homeMapView';

export function saveMapView(view: SavedMapView) {
  sessionStorage.setItem(MAP_VIEW_STORAGE_KEY, JSON.stringify(view));
}

// Consumes the saved view (clears it after reading) -- a plain fresh visit
// later shouldn't keep re-applying an old camera position from a past
// session.
export function takeSavedMapView(): SavedMapView | null {
  const raw = sessionStorage.getItem(MAP_VIEW_STORAGE_KEY);
  if (!raw) return null;
  sessionStorage.removeItem(MAP_VIEW_STORAGE_KEY);
  try {
    return JSON.parse(raw) as SavedMapView;
  } catch {
    return null;
  }
}
