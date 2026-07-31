/**
 * Provider detail page slugs are `{name-separated-by-hyphens}-{achc_source_id}`.
 * achc_source_id is ACHC's own stable identifier (not one we invented), which
 * is what actually makes the slug unique -- provider names alone repeat.
 */

export function buildProviderSlug(name: string, achcSourceId: string | null | undefined): string {
  const namePart = (name || 'provider')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return achcSourceId ? `${namePart}-${achcSourceId}` : namePart;
}

/** Extracts the achc_source_id from a slug -- everything after the last hyphen. */
export function parseAchcSourceIdFromSlug(slug: string): string | null {
  const match = slug.match(/-(\d+)$/);
  return match ? match[1] : null;
}
