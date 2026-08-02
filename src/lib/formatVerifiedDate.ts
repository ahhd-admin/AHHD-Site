// Trust signal shown alongside every listing: when this provider's data was
// last confirmed against its accrediting source, so a visitor doesn't have
// to wonder whether they're looking at stale info. source_last_seen_at is
// the nightly scraper's own "still listed at the source as of this run"
// timestamp (refreshed on every run that finds the record, not just once at
// import) -- a truer "recently confirmed" signal than last_verified_at,
// which in practice is set once at import and not kept current, or
// updated_at, which also bumps for unrelated internal writes.
export function getVerifiedDate(location: {
  source_last_seen_at?: string;
  updated_at?: string;
  last_verified_at?: string;
}): string | null {
  return location.source_last_seen_at || location.updated_at || location.last_verified_at || null;
}

export function formatVerifiedDate(dateString: string | null | undefined): string | null {
  if (!dateString) return null;
  const date = new Date(dateString);
  if (Number.isNaN(date.getTime())) return null;

  // Always an explicit calendar date, not relative phrasing ("yesterday",
  // "3 days ago") -- an exact date reads as a verifiable fact a visitor
  // can check for themselves; "yesterday" reads as vaguer, and stays
  // exactly as vague however long the underlying data actually goes
  // unrefreshed (worth being precise about, since the nightly scraper
  // pipeline that's meant to keep this current isn't live yet as of
  // 2026-08-01 -- see MVP-Rollout-Roadmap.md).
  return `Last verified: ${date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}`;
}
