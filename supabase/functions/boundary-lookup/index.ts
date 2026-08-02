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
// transfer and render. Raised from 400 (2026-08-02): confirmed live
// that 400 was still visibly too coarse along a shared state border --
// part of Oklahoma's traced edge cut into Texas instead of following
// the real line between them. Real Douglas-Peucker (see below) makes
// this a genuine quality/size tradeoff now rather than a bug either
// value would still have -- 1000 gives noticeably more fidelity along
// long shared borders while staying well under the ~3800 raw point
// count of the most complex real shapes.
const MAX_POINTS_PER_RING = 1000;

// History of two wrong attempts at this before landing on real
// Douglas-Peucker simplification, both confirmed live against Texas's
// panhandle:
//
// 1. Plain every-Nth-index decimation dropped the panhandle tip
//    entirely -- its northmost point just didn't happen to land on a
//    kept index.
// 2. Fixing #1 by force-keeping the 4 global extrema (north/south/east/
//    west-most points) stopped the tip from vanishing, but the
//    panhandle's actual RECTANGULAR shape still came out wrong -- one
//    top corner survived (it happened to be the true northmost point),
//    but the other top corner is a separate vertex nowhere near the
//    global north/south/east/west extremes, so it kept getting
//    decimated away, leaving a single sharp point instead of a flat
//    top edge.
//
// Global extrema can never fix this class of bug -- a shape's
// significant vertices (corners, protrusions) aren't necessarily the
// polygon's overall min/max points. Real Douglas-Peucker simplification
// (recursively keeping whichever point deviates most from the current
// simplified line, for every subsegment, not just the 4 global
// extremes) is the actual correct algorithm for this and is what should
// have been used from the start.
function perpendicularDistance(point: number[], lineStart: number[], lineEnd: number[]): number {
  const [px, py] = point;
  const [x1, y1] = lineStart;
  const [x2, y2] = lineEnd;
  const dx = x2 - x1;
  const dy = y2 - y1;
  if (dx === 0 && dy === 0) {
    return Math.hypot(px - x1, py - y1);
  }
  const t = ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy);
  const closestX = x1 + t * dx;
  const closestY = y1 + t * dy;
  return Math.hypot(px - closestX, py - closestY);
}

function douglasPeucker(points: number[][], epsilon: number): number[][] {
  if (points.length < 3) return points;
  let maxDist = 0;
  let index = 0;
  const end = points.length - 1;
  for (let i = 1; i < end; i++) {
    const dist = perpendicularDistance(points[i], points[0], points[end]);
    if (dist > maxDist) {
      maxDist = dist;
      index = i;
    }
  }
  if (maxDist > epsilon) {
    const left = douglasPeucker(points.slice(0, index + 1), epsilon);
    const right = douglasPeucker(points.slice(index), epsilon);
    return left.slice(0, -1).concat(right);
  }
  return [points[0], points[end]];
}

// Douglas-Peucker's epsilon (max allowed deviation) doesn't map directly
// to an output point count, so binary-search it until the result is at
// or under the target -- guarantees the size cap while still using real
// shape-aware simplification rather than index-based sampling.
function thinRing(points: number[][]): number[][] {
  if (points.length <= MAX_POINTS_PER_RING) return points;

  let lo = 0;
  let hi = 5; // degrees -- far larger than any single ring would ever need
  let best = douglasPeucker(points, hi);

  for (let iter = 0; iter < 20; iter++) {
    const mid = (lo + hi) / 2;
    const result = douglasPeucker(points, mid);
    if (result.length > MAX_POINTS_PER_RING) {
      lo = mid;
    } else {
      hi = mid;
      best = result;
    }
    if (hi - lo < 1e-6) break;
  }

  return best;
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
