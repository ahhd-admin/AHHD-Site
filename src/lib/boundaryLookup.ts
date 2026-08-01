import { supabase } from './supabase';

export type BoundaryGeoJSON =
  | { type: 'Polygon'; coordinates: number[][][] }
  | { type: 'MultiPolygon'; coordinates: number[][][][] };

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
