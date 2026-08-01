import { supabase } from './supabase';

export type BoundaryGeoJSON =
  | { type: 'Polygon'; coordinates: number[][][] }
  | { type: 'MultiPolygon'; coordinates: number[][][][] };

// All 50 states' real traced outlines, pre-fetched once (see
// ~/web-testing/fetch_state_boundaries.mjs) via the same boundary-lookup
// Edge Function/Nominatim path used for on-demand traces, and bundled as a
// static public asset -- a state search is always one of a fixed 50
// regions, unlike arbitrary city/zip text, so there's no reason to hit
// Nominatim live (and wait on it, and depend on it being up) every time
// someone searches a state. Fetched once per page load and cached in this
// module-level variable -- 50 states is one ~650KB JSON file, small enough
// to keep entirely in memory rather than re-fetching per search.
let stateBoundariesPromise: Promise<Record<string, BoundaryGeoJSON>> | null = null;

function loadStateBoundaries(): Promise<Record<string, BoundaryGeoJSON>> {
  if (!stateBoundariesPromise) {
    stateBoundariesPromise = fetch('/data/stateBoundaries.json')
      .then((res) => (res.ok ? res.json() : {}))
      .catch(() => ({}));
  }
  return stateBoundariesPromise;
}

// Looks up a state's pre-fetched boundary by its 2-letter code. Returns
// null on a cache miss (a state missing from the bundle, or the bundle
// failing to load) so the caller can fall back to the live Nominatim
// lookup instead of showing nothing.
export async function fetchStateBoundary(stateCode: string): Promise<BoundaryGeoJSON | null> {
  const boundaries = await loadStateBoundaries();
  return boundaries[stateCode.toUpperCase()] ?? null;
}

// Walks every ring in a Polygon/MultiPolygon (coordinates are [lng, lat],
// the GeoJSON standard order -- opposite of the {lat, lng} object shape
// used elsewhere in this codebase) to find its real bounding box. Lets a
// direct state-name match (see SearchHero.tsx's short-circuit around
// resolveStateCode) get real bounds/center straight from the boundary
// polygon already being fetched for the outline, without a separate
// geocoding call.
export function computeBoundsFromGeoJSON(
  geo: BoundaryGeoJSON
): { south: number; west: number; north: number; east: number } {
  let south = Infinity, west = Infinity, north = -Infinity, east = -Infinity;
  const visitRing = (ring: number[][]) => {
    for (const [lng, lat] of ring) {
      if (lat < south) south = lat;
      if (lat > north) north = lat;
      if (lng < west) west = lng;
      if (lng > east) east = lng;
    }
  };
  if (geo.type === 'Polygon') {
    geo.coordinates.forEach(visitRing);
  } else {
    geo.coordinates.forEach((polygon) => polygon.forEach(visitRing));
  }
  return { south, west, north, east };
}

// Fetches the real traced boundary shape for a search (a city's actual
// outline, a state's actual outline) via the boundary-lookup Edge
// Function, which proxies OpenStreetMap/Nominatim server-side (Nominatim
// itself doesn't send CORS headers, so the browser can't call it
// directly -- verified by testing it directly). Returns null on ANY
// failure -- network error, function down, Nominatim down, timeout, or
// no boundary found for this search (e.g. most ZIP codes aren't mapped
// as polygons in OSM) -- by design: a visitor's search should never
// break, or even feel slower, because this optional, best-effort lookup
// didn't work. The caller falls back to the existing rectangle/circle
// region indicator in that case.
export async function fetchSearchBoundary(query: string): Promise<BoundaryGeoJSON | null> {
  const trimmed = query.trim();
  if (!trimmed) return null;

  try {
    const { data, error } = await supabase.functions.invoke(
      `boundary-lookup?q=${encodeURIComponent(trimmed)}`,
      { method: 'GET', timeout: 5000 }
    );

    if (error || !data || !data.geojson) return null;

    const geojson = data.geojson as BoundaryGeoJSON;
    if (geojson.type !== 'Polygon' && geojson.type !== 'MultiPolygon') return null;

    return geojson;
  } catch {
    return null;
  }
}
