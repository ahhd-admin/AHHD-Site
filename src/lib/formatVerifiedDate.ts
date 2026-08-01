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

const RECENT_THRESHOLD_DAYS = 3;

export function formatVerifiedDate(dateString: string | null | undefined): string | null {
  if (!dateString) return null;
  const date = new Date(dateString);
  if (Number.isNaN(date.getTime())) return null;

  // "Last verified:" instead of just "Verified" -- reads as a live,
  // computed status (this listing was actually re-checked) rather than
  // static copy baked into the page.
  const daysAgo = Math.floor((Date.now() - date.getTime()) / (1000 * 60 * 60 * 24));
  if (daysAgo <= 0) return 'Last verified: today';
  if (daysAgo === 1) return 'Last verified: yesterday';
  if (daysAgo <= RECENT_THRESHOLD_DAYS) return `Last verified: ${daysAgo} days ago`;

  return `Last verified: ${date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}`;
}
