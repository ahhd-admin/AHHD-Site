// Server-side proxy to Nominatim (OpenStreetMap) for real administrative
// boundary shapes (a city's actual traced outline, not just a rectangle).
// Exists because Nominatim's public API doesn't send CORS headers --
// verified directly (curl reaches it fine; a browser fetch() would be
// silently blocked) -- so the browser can't call it itself. This function
// makes the request server-to-server (not subject to CORS) and returns
// just the boundary coordinates the client needs.
//
// No API key/account needed for Nominatim -- it's a free public service,
// but its usage policy requires requests to identify the calling
// application via a User-Agent header, which only a server can reliably
// set (browsers block scripts from overriding that header).

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

const NOMINATIM_USER_AGENT =
  'AHHD-Site/1.0 (Accredited Home Healthcare Directory -- www.achhd.org)';

// Nominatim boundaries can run to several thousand points (Chicago's is
// ~3800) -- far more than needed for a smooth-looking outline at any
// zoom level a visitor would realistically use, and needlessly heavy to
// transfer and render. Simple decimation (not true Douglas-Peucker
// simplification, which needs a dedicated library) keeps the response
// small while still tracing the real shape.
const MAX_POINTS_PER_RING = 400;

function thinRing(points: number[][]): number[][] {
  if (points.length <= MAX_POINTS_PER_RING) return points;
  const step = Math.ceil(points.length / MAX_POINTS_PER_RING);
  const thinned = points.filter((_, i) => i % step === 0);
  const last = points[points.length - 1];
  if (thinned[thinned.length - 1] !== last) thinned.push(last);
  return thinned;
}

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response(null, { headers: CORS_HEADERS });
  }

  try {
    const url = new URL(req.url);
    const q = url.searchParams.get('q');
    if (!q || !q.trim()) {
      return new Response(JSON.stringify({ error: 'Missing q parameter' }), {
        status: 400,
        headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
      });
    }

    const nominatimUrl = `https://nominatim.openstreetmap.org/search?${new URLSearchParams({
      q,
      format: 'json',
      polygon_geojson: '1',
      limit: '1',
      addressdetails: '0',
    })}`;

    const response = await fetch(nominatimUrl, {
      headers: { 'User-Agent': NOMINATIM_USER_AGENT },
    });

    if (!response.ok) {
      return new Response(JSON.stringify({ geojson: null }), {
        headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
      });
    }

    const results = await response.json();
    const geojson = results?.[0]?.geojson;

    // Nominatim can also return Point/LineString for some queries (e.g. a
    // street address with no enclosing boundary) -- only Polygon/
    // MultiPolygon are actually traceable outlines.
    if (!geojson || (geojson.type !== 'Polygon' && geojson.type !== 'MultiPolygon')) {
      return new Response(JSON.stringify({ geojson: null }), {
        headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
      });
    }

    const thinnedGeojson =
      geojson.type === 'Polygon'
        ? { type: 'Polygon', coordinates: geojson.coordinates.map(thinRing) }
        : {
            type: 'MultiPolygon',
            coordinates: geojson.coordinates.map((polygon: number[][][]) => polygon.map(thinRing)),
          };

    return new Response(JSON.stringify({ geojson: thinnedGeojson }), {
      headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  } catch (error) {
    console.error('boundary-lookup error:', error);
    // A visitor's search should never break because this optional lookup
    // failed -- respond with "no boundary found" rather than an error
    // status, so the client's fallback path (see boundaryLookup.ts) is
    // the same code path as "Nominatim just didn't have this place."
    return new Response(JSON.stringify({ geojson: null }), {
      headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' },
    });
  }
});
