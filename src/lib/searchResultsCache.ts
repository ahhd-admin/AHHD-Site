import type { LocationWithDetails } from '../types/database';

// Provider links are real page loads (see scrollRestoration.ts), so
// returning from a detail page re-mounts SearchHero from scratch and
// would normally re-run the full geocode + Supabase fetch. Caching the
// last search's actual results (plus the coordinates/bounds the map
// needs to re-center) lets that restore be instant and network-free
// instead of visibly reloading/repopulating.
const CACHE_KEY = 'ahhd:searchCache';
// Long enough to survive a quick detour to a provider page and back;
// short enough that genuinely stale data won't linger across a whole
// session.
const MAX_AGE_MS = 10 * 60 * 1000;

interface CachedSearchBounds {
  south: number;
  west: number;
  north: number;
  east: number;
}

interface CachedSearchEntry {
  key: string;
  results: LocationWithDetails[];
  userCoords: { lat: number; lng: number } | null;
  searchBounds: CachedSearchBounds | null;
  savedAt: number;
}

function buildKey(location: string, services: string[]): string {
  return `${location.trim().toLowerCase()}|${[...services].sort().join(',')}`;
}

export function saveSearchCache(
  location: string,
  services: string[],
  results: LocationWithDetails[],
  userCoords: { lat: number; lng: number } | null,
  searchBounds: CachedSearchBounds | null
) {
  try {
    const entry: CachedSearchEntry = {
      key: buildKey(location, services),
      results,
      userCoords,
      searchBounds,
      savedAt: Date.now(),
    };
    sessionStorage.setItem(CACHE_KEY, JSON.stringify(entry));
  } catch {
    // Quota exceeded or storage unavailable -- caching is a nice-to-have
    // speedup, never something that should break the search itself.
  }
}

export function loadSearchCache(
  location: string,
  services: string[]
): { results: LocationWithDetails[]; userCoords: { lat: number; lng: number } | null; searchBounds: CachedSearchBounds | null } | null {
  try {
    const raw = sessionStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const entry: CachedSearchEntry = JSON.parse(raw);
    if (entry.key !== buildKey(location, services)) return null;
    if (Date.now() - entry.savedAt > MAX_AGE_MS) return null;
    return { results: entry.results, userCoords: entry.userCoords, searchBounds: entry.searchBounds };
  } catch {
    return null;
  }
}
